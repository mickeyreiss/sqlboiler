package core

import (
	"github.com/mickeyreiss/sqlgen/db"
	"io"
)

type TableRenderer interface {
	Render(table db.Table, w io.Writer) error
}

type TableTestRenderer interface {
	RenderTest(table db.Table, w io.Writer) error
}
