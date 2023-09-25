package snowflake

import (
	"strings"
	"testing"

	"github.com/ytake/p2t/reader"
	"github.com/ytake/p2t/value"
)

func TestTableResourceColumn_Name(t *testing.T) {
	t.Run("with varchar", func(t *testing.T) {
		col := TableResourceColumn{}.Name(Column{Name: "test_id", Type: "VARCHAR"})
		if !strings.Contains(col, "type = \"VARCHAR\"") {
			t.Errorf("got %v\nwant %v", col, "VARCHAR")
		}
	})
	t.Run("with timestamp_ntz", func(t *testing.T) {
		col := TableResourceColumn{}.Name(Column{Name: "test_created_at", Type: "VARCHAR"})
		if !strings.Contains(col, "type = \"TIMESTAMP_NTZ(9)\"") {
			t.Errorf("got %v\nwant %v", col, "TIMESTAMP_NTZ(9)")
		}
	})
}

func TestCreateTableResource_Generate(t *testing.T) {
	pf, err := reader.Parquet{}.Open("../testdata/test.parquet")
	if err != nil {
		t.Fatal(err)
	}
	s := NewDDL(pf.Schema(), value.TerraformTableResource).Transform()
	expect := `resource "snowflake_table" "replace_me" {
  database = snowflake_schema.schema.database
  schema   = snowflake_schema.schema.name
  provider = your_snowflake_provider
  name     = "replace me"
  comment  = "A table."

  column {
    name = "REGISTRATION_DTTM"
    type = "NUMBER(38,0)"
  }

  column {
    name = "ID"
    type = "NUMBER(38,0)"
  }

  column {
    name = "FIRST_NAME"
    type = "VARCHAR"
  }

  column {
    name = "LAST_NAME"
    type = "VARCHAR"
  }

  column {
    name = "EMAIL"
    type = "VARCHAR"
  }

  column {
    name = "GENDER"
    type = "VARCHAR"
  }

  column {
    name = "IP_ADDRESS"
    type = "VARCHAR"
  }

  column {
    name = "CC"
    type = "VARCHAR"
  }

  column {
    name = "COUNTRY"
    type = "VARCHAR"
  }

  column {
    name = "BIRTHDATE"
    type = "DATE"
  }

  column {
    name = "SALARY"
    type = "FLOAT"
  }

  column {
    name = "TITLE"
    type = "VARCHAR"
  }

  column {
    name = "COMMENTS"
    type = "VARCHAR"
  }

  column {
    name = "FILENAME"
    type = "VARCHAR"
  }

  column {
    name = "FILE_ROW_NUMBER"
    type = "NUMBER(38,0)"
  }

  column {
    name = "FILE_CONTENT_KEY"
    type = "VARCHAR"
  }

  column {
    name = "FILE_LAST_MODIFIED"
    type = "TIMESTAMP_NTZ(9)"
  }

  column {
    name = "START_SCAN_TIME"
    type = "TIMESTAMP_LTZ(9)"
  }
}`

	if s != expect {
		t.Errorf("got %v\nwant %v", s, expect)
	}
}
