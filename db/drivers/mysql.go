package drivers

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/mickeyreiss/sqlgen/db"
	"github.com/pkg/errors"
)

// TinyintAsBool is a global that is set from main.go if a user specifies
// this flag when generating. This flag only applies to MySQL so we're using
// a global instead, to avoid breaking the interface. If TinyintAsBool is true
// then tinyint(1) will be mapped in your generated structs to bool opposed to int8.
var TinyintAsBool bool

// MySQLDriver holds the database connection string and a handle
// to the database connection.
type MySQLDriver struct {
	connStr string
	dbConn  *sql.DB
}

// NewMySQLDriver takes the database connection details as parameters and
// returns a pointer to a MySQLDriver object. Note that it is required to
// call MySQLDriver.Open() and MySQLDriver.Close() to open and close
// the database connection once an object has been obtained.
func NewMySQLDriver(user, pass, dbname, host string, port int, sslmode string) *MySQLDriver {
	driver := MySQLDriver{
		connStr: MySQLBuildQueryString(user, pass, dbname, host, port, sslmode),
	}

	return &driver
}

// MySQLBuildQueryString builds a query string for MySQL.
func MySQLBuildQueryString(user, pass, dbname, host string, port int, sslmode string) string {
	var config mysql.Config

	config.User = user
	if len(pass) != 0 {
		config.Passwd = pass
	}
	config.DBName = dbname
	config.Net = "tcp"
	config.Addr = host
	if port == 0 {
		port = 3306
	}
	config.Addr += ":" + strconv.Itoa(port)
	config.TLSConfig = sslmode

	// MySQL is a bad, and by default reads date/datetime into a []byte
	// instead of a time.Time. Tell it to stop being a bad.
	config.ParseTime = true

	return config.FormatDSN()
}

// Open opens the database connection using the connection string
func (m *MySQLDriver) Open() error {
	var err error
	m.dbConn, err = sql.Open("mysql", m.connStr)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the database connection
func (m *MySQLDriver) Close() {
	m.dbConn.Close()
}

// UseLastInsertID returns false for postgres
func (m *MySQLDriver) UseLastInsertID() bool {
	return true
}

// UseTopClause returns false to indicate MySQL doesnt support SQL TOP clause
func (m *MySQLDriver) UseTopClause() bool {
	return false
}

// TableNames connects to the postgres database and
// retrieves all table names from the information_schema where the
// table schema is public.
func (m *MySQLDriver) TableNames(schema string, whitelist, blacklist []string) ([]string, error) {
	var names []string

	query := fmt.Sprintf(`select table_name from information_schema.tables where table_schema = ? and table_type = 'BASE TABLE'`)
	args := []interface{}{schema}
	if len(whitelist) > 0 {
		query += fmt.Sprintf(" and table_name in (%s);", strings.Repeat(",?", len(whitelist))[1:])
		for _, w := range whitelist {
			args = append(args, w)
		}
	} else if len(blacklist) > 0 {
		query += fmt.Sprintf(" and table_name not in (%s);", strings.Repeat(",?", len(blacklist))[1:])
		for _, b := range blacklist {
			args = append(args, b)
		}
	}

	rows, err := m.dbConn.Query(query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}

	return names, nil
}

// Columns takes a table name and attempts to retrieve the table information
// from the database information_schema.columns. It retrieves the column names
// and column types and returns those as a []Column after TranslateColumnType()
// converts the SQL types to Go types, for example: "varchar" to "string"
func (m *MySQLDriver) Columns(schema, tableName string) ([]db.Column, error) {
	var columns []db.Column

	rows, err := m.dbConn.Query(`
	select
	c.column_name,
	c.column_type,
	if(c.data_type = 'enum', c.column_type, c.data_type),
	if(extra = 'auto_increment','auto_increment', c.column_default),
	c.is_nullable = 'YES',
	c.column_type LIKE '% unsigned',
		exists (
			select c.column_name
			from information_schema.table_constraints tc
			inner join information_schema.key_column_usage kcu
				on tc.constraint_name = kcu.constraint_name and tc.table_name = kcu.table_name and tc.table_schema = kcu.table_schema
			where c.column_name = kcu.column_name and tc.table_name = c.table_name and
				(tc.constraint_type = 'PRIMARY KEY' or tc.constraint_type = 'UNIQUE') and
				(select count(*) from information_schema.key_column_usage where table_schema = kcu.table_schema and table_name = tc.table_name and constraint_name = tc.constraint_name) = 1
		) as is_unique
	from information_schema.columns as c
	where table_name = ? and table_schema = ?;
	`, tableName, schema)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var colName, colType, colFullType string
		var nullable, unsigned, unique bool
		var defaultValue *string
		if err := rows.Scan(&colName, &colFullType, &colType, &defaultValue, &nullable, &unsigned, &unique); err != nil {
			return nil, errors.Wrapf(err, "unable to scan for table %s", tableName)
		}

		column := db.Column{
			Name:       colName,
			FullDBType: colFullType, // example: tinyint(1) instead of tinyint
			DBType:     colType,
			Nullable:   nullable,
			Unsigned:   unsigned,
			Unique:     unique,
		}

		if defaultValue != nil && *defaultValue != "NULL" {
			column.Default = *defaultValue
		}

		columns = append(columns, column)
	}

	return columns, nil
}

// PrimaryKeyInfo looks up the primary key for a table.
func (m *MySQLDriver) PrimaryKeyInfo(schema, tableName string) (*db.PrimaryKey, error) {
	pkey := &db.PrimaryKey{}
	var err error

	query := `
	select tc.constraint_name
	from information_schema.table_constraints as tc
	where tc.table_name = ? and tc.constraint_type = 'PRIMARY KEY' and tc.table_schema = ?;`

	row := m.dbConn.QueryRow(query, tableName, schema)
	if err = row.Scan(&pkey.Name); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	queryColumns := `
	select kcu.column_name
	from   information_schema.key_column_usage as kcu
	where  table_name = ? and constraint_name = ? and table_schema = ?;`

	var rows *sql.Rows
	if rows, err = m.dbConn.Query(queryColumns, tableName, pkey.Name, schema); err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string

		err = rows.Scan(&column)
		if err != nil {
			return nil, err
		}

		columns = append(columns, column)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	pkey.Columns = columns

	return pkey, nil
}

// ForeignKeyInfo retrieves the foreign keys for a given table name.
func (m *MySQLDriver) ForeignKeyInfo(schema, tableName string) ([]db.ForeignKey, error) {
	var fkeys []db.ForeignKey

	query := `
	select constraint_name, table_name, column_name, referenced_table_name, referenced_column_name
	from information_schema.key_column_usage
	where table_schema = ? and referenced_table_schema = ? and table_name = ?
	`

	var rows *sql.Rows
	var err error
	if rows, err = m.dbConn.Query(query, schema, schema, tableName); err != nil {
		return nil, err
	}

	for rows.Next() {
		var fkey db.ForeignKey
		var sourceTable string

		fkey.Table = tableName
		err = rows.Scan(&fkey.Name, &sourceTable, &fkey.Column, &fkey.ForeignTable, &fkey.ForeignColumn)
		if err != nil {
			return nil, err
		}

		fkeys = append(fkeys, fkey)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return fkeys, nil
}

// TranslateColumnType converts postgres database types to Go types, for example
// "varchar" to "string" and "bigint" to "int64". It returns this parsed data
// as a Column object.
func (m *MySQLDriver) TranslateColumnType(c db.Column) db.Column {
	if c.Nullable {
		switch c.DBType {
		case "tinyint":
			// map tinyint(1) to bool if TinyintAsBool is true
			if TinyintAsBool && c.FullDBType == "tinyint(1)" {
				c.PkgName = "gopkg.in/nullbio/null.v6"
				c.TypeName = "Bool"
			} else {
				if c.Unsigned {
					c.PkgName = "gopkg.in/nullbio/null.v6"
					c.TypeName = "Uint8"
				} else {
					c.PkgName = "gopkg.in/nullbio/null.v6"
					c.TypeName = "Int8"
				}
			}
		case "smallint":
			if c.Unsigned {
				c.PkgName = "gopkg.in/nullbio/null.v6"
				c.TypeName = "Uint16"
			} else {
				c.PkgName = "gopkg.in/nullbio/null.v6"
				c.TypeName = "Int16"
			}
		case "mediumint":
			if c.Unsigned {
				c.PkgName = "gopkg.in/nullbio/null.v6"
				c.TypeName = "Uint32"
			} else {
				c.PkgName = "gopkg.in/nullbio/null.v6"
				c.TypeName = "Int32"
			}
		case "int", "integer":
			if c.Unsigned {
				c.PkgName = "gopkg.in/nullbio/null.v6"
				c.TypeName = "Uint"
			} else {
				c.PkgName = "gopkg.in/nullbio/null.v6"
				c.TypeName = "Int"
			}
		case "bigint":
			if c.Unsigned {
				c.PkgName = "gopkg.in/nullbio/null.v6"
				c.TypeName = "Uint64"
			} else {
				c.PkgName = "gopkg.in/nullbio/null.v6"
				c.TypeName = "Int64"
			}
		case "float":
			c.PkgName = "gopkg.in/nullbio/null.v6"
			c.TypeName = "Float32"
		case "double", "double precision", "real":
			c.PkgName = "gopkg.in/nullbio/null.v6"
			c.TypeName = "Float64"
		case "boolean", "bool":
			c.PkgName = "gopkg.in/nullbio/null.v6"
			c.TypeName = "Bool"
		case "date", "datetime", "timestamp", "time":
			c.PkgName = "gopkg.in/nullbio/null.v6"
			c.TypeName = "Time"
		case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
			c.PkgName = "gopkg.in/nullbio/null.v6"
			c.TypeName = "Bytes"
		case "json":
			c.PkgName = "github.com/vattle/sqlboiler/types"
			c.TypeName = "JSON"
		default:
			c.PkgName = "gopkg.in/nullbio/null.v6"
			c.TypeName = "String"
		}
	} else {
		switch c.DBType {
		case "tinyint":
			// map tinyint(1) to bool if TinyintAsBool is true
			if TinyintAsBool && c.FullDBType == "tinyint(1)" {
				c.TypeName = "bool"
			} else {
				if c.Unsigned {
					c.TypeName = "uint8"
				} else {
					c.TypeName = "int8"
				}
			}
		case "smallint":
			if c.Unsigned {
				c.TypeName = "uint16"
			} else {
				c.TypeName = "int16"
			}
		case "mediumint":
			if c.Unsigned {
				c.TypeName = "uint32"
			} else {
				c.TypeName = "int32"
			}
		case "int", "integer":
			if c.Unsigned {
				c.TypeName = "uint"
			} else {
				c.TypeName = "int"
			}
		case "bigint":
			if c.Unsigned {
				c.TypeName = "uint64"
			} else {
				c.TypeName = "int64"
			}
		case "float":
			c.TypeName = "float32"
		case "double", "double precision", "real":
			c.TypeName = "float64"
		case "boolean", "bool":
			c.TypeName = "bool"
		case "date", "datetime", "timestamp", "time":
			c.PkgName = "time"
			c.TypeName = "Time"
		case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
			c.TypeName = "[]byte"
		case "json":
			c.PkgName = "github.com/vattle/sqlboiler/types"
			c.TypeName = "JSON"
		default:
			c.TypeName = "string"
		}
	}

	return c
}

// RightQuote is the quoting character for the right side of the identifier
func (m *MySQLDriver) RightQuote() byte {
	return '`'
}

// LeftQuote is the quoting character for the left side of the identifier
func (m *MySQLDriver) LeftQuote() byte {
	return '`'
}

// IndexPlaceholders returns false to indicate MySQL doesnt support indexed placeholders
func (m *MySQLDriver) IndexPlaceholders() bool {
	return false
}
