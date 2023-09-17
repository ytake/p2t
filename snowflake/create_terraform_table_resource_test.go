package snowflake

import (
	"testing"

	"github.com/ytake/p2t/reader"
	"github.com/ytake/p2t/value"
)

func TestTableResourceColumn_Name(t *testing.T) {
	expect := `column {
  name = "TEST_ID"
  type = "VARCHAR"
  nullable = false
}`
	col := TableResourceColumn{}.Name(Column{Name: "test_id", Type: "VARCHAR"})
	if col != expect {
		t.Errorf("got %v\nwant %v", col, expect)
	}
	col = TableResourceColumn{}.Name(Column{Name: "test_created_at", Type: "VARCHAR"})
	expect = `column {
  name = "TEST_CREATED_AT"
  type = "TIMESTAMP"
  nullable = false
}`
	if col != expect {
		t.Errorf("got %v\nwant %v", col, expect)
	}
}

func TestCreateTableResource_Generate(t *testing.T) {
	pf, err := reader.Parquet{}.Open("../example/test.parquet")
	if err != nil {
		t.Fatal(err)
	}
	s := NewDDL(pf.Schema(), value.TerraformTableResource).Transform()
	expect := `resource "snowflake_table" "replace_me" {
  database            = snowflake_schema.schema.database
  schema              = snowflake_schema.schema.name
  name                = "replace me"
  comment             = "A table."

  column {
    name = "REGISTRATION_DTTM"
    type = "NUMBER"
  }

  column {
    name = "ID"
    type = "NUMBER"
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
}`

	if s != expect {
		t.Errorf("got %v\nwant %v", s, expect)
	}
}
