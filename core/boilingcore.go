// Package boilingcore has types and methods useful for generating code that
// acts as a fully dynamic ORM might.
package core

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	db "github.com/mickeyreiss/sqlgen/db"
	"github.com/mickeyreiss/sqlgen/db/drivers"
	"github.com/pkg/errors"
)

// State holds the global data needed by most pieces to run
type State struct {
	Config *Config

	Driver db.Interface
	Tables []db.Table
}

// New creates a new state based off of the config
func New(config *Config) (*State, error) {
	s := &State{
		Config: config,
	}

	err := s.initDriver(config.DriverName)
	if err != nil {
		return nil, err
	}

	if s.Config.TableRenderer == nil {
		return nil, errors.New("sqlboiler config must specify a TableRenderer")
	}

	return s, nil
}

// templateData for sqlboiler templates
type TemplateData struct {
	Tables []db.Table
	Table  db.Table

	// Controls what names are output
	PkgName string
	Schema  string
}
type TableRenderer interface {
	Render(config TemplateData, w io.Writer) error
}

type TableTestRenderer interface {
	RenderTest(config TemplateData, w io.Writer) error
}

// Run executes the sqlboiler templates and outputs them to files based on the
// state given.
func (s *State) Run() error {
	var err error
	// Connect to the driver database
	if err = s.Driver.Open(); err != nil {
		return errors.Wrap(err, "unable to connect to the database")
	}

	err = s.initTables(s.Config.Schema, s.Config.WhitelistTables, s.Config.BlacklistTables)
	if err != nil {
		return errors.Wrap(err, "unable to initialize tables")
	}

	if s.Config.Debug {
		b, err := json.Marshal(s.Tables)
		if err != nil {
			return errors.Wrap(err, "unable to json marshal tables")
		}
		fmt.Printf("%s\n", b)
	}

	err = s.initOutFolder()
	if err != nil {
		return errors.Wrap(err, "unable to initialize the output folder")
	}

	//if !s.Config.NoTests {
	//	if err := generateTestMainOutput(s, singletonData); err != nil {
	//		return errors.Wrap(err, "unable to generate TestMain output")
	//	}
	//}
	for _, table := range s.Tables {

		data := TemplateData{
			Tables:  s.Tables,
			Table:   table,
			Schema:  s.Config.Schema,
			PkgName: s.Config.PkgName,
		}

		if table.IsJoinTable {
			continue
		}

		if err := func(data TemplateData) error {
			// Open model file.
			w, err := s.openFile(data.Table.Name, "_gen.go")
			if err != nil {
				panic(err)
			}
			defer w.Close()

			// Generate the table templates
			if err := s.Config.TableRenderer.Render(data, w); err != nil {
				return errors.Wrap(err, "unable to generate output")
			}

			return nil
		}(data); err != nil {
			return err
		}

		if testRenderer := s.Config.TableTestRenderer; !s.Config.NoTests && testRenderer != nil {
			if err := func(data TemplateData) error {
				// Open model test file.
				w, err := s.openFile(table.Name, "_test_gen.go")
				if err != nil {
					panic(err)
				}
				defer w.Close()

				// Generate the test templates
				if err := testRenderer.RenderTest(data, w); err != nil {
					return errors.Wrap(err, "unable to generate test output")
				}
				return nil
			}(data); err != nil {
				return err
			}
		}
	}

	return nil
}

// openFile opens a file for rendering a go file.
func (s *State) openFile(filename, suffix string) (*os.File, error) {
	path := filepath.Join(s.Config.OutFolder, filename)
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, err
	}
	w, err := os.OpenFile(filepath.Join(path, filename+suffix), os.O_WRONLY|os.O_CREATE, 0444)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// Cleanup closes any resources that must be closed
func (s *State) Cleanup() error {
	s.Driver.Close()
	return nil
}

// initDriver attempts to set the state Interface based off the passed in
// driver flag value. If an invalid flag string is provided an error is returned.
func (s *State) initDriver(driverName string) error {
	// Create a driver based off driver flag
	switch driverName {
	case "postgres":
		s.Driver = drivers.NewPostgresDriver(
			s.Config.Postgres.User,
			s.Config.Postgres.Pass,
			s.Config.Postgres.DBName,
			s.Config.Postgres.Host,
			s.Config.Postgres.Port,
			s.Config.Postgres.SSLMode,
		)
	case "mysql":
		s.Driver = drivers.NewMySQLDriver(
			s.Config.MySQL.User,
			s.Config.MySQL.Pass,
			s.Config.MySQL.DBName,
			s.Config.MySQL.Host,
			s.Config.MySQL.Port,
			s.Config.MySQL.SSLMode,
		)
	case "mock":
		s.Driver = &drivers.MockDriver{}
	}

	if s.Driver == nil {
		return errors.New("An invalid driver name was provided")
	}

	return nil
}

// initTables retrieves all "public" schema table names from the database.
func (s *State) initTables(schema string, whitelist, blacklist []string) error {
	var err error
	s.Tables, err = db.Tables(s.Driver, schema, whitelist, blacklist)
	if err != nil {
		return errors.Wrap(err, "unable to fetch table data")
	}

	if len(s.Tables) == 0 {
		return errors.New("no tables found in database")
	}

	if err := checkPKeys(s.Tables); err != nil {
		return err
	}

	return nil
}

// initOutFolder creates the folder that will hold the generated output.
func (s *State) initOutFolder() error {
	if s.Config.Wipe {
		if err := os.RemoveAll(s.Config.OutFolder); err != nil {
			return err
		}
	}

	return os.MkdirAll(s.Config.OutFolder, os.ModePerm)
}

// checkPKeys ensures every table has a primary key column
func checkPKeys(tables []db.Table) error {
	var missingPkey []string
	for _, t := range tables {
		if t.PKey == nil {
			missingPkey = append(missingPkey, t.Name)
		}
	}

	if len(missingPkey) != 0 {
		return errors.Errorf("primary key missing in tables (%s)", strings.Join(missingPkey, ", "))
	}

	return nil
}
