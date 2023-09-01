package snowflake

import (
	"testing"

	"github.com/ytake/p2t/reader"
	"github.com/ytake/p2t/value"
)

func TestNameWithType_Name(t *testing.T) {
	expect := "TEST_ID VARCHAR"
	col := NameWithType{}.Name(Column{Name: "test_id", Type: "VARCHAR"})
	if col != expect {
		t.Errorf("got %v\nwant %v", col, expect)
	}
	col = NameWithType{}.Name(Column{Name: "test_created_at", Type: "VARCHAR"})
	expect = "TEST_CREATED_AT VARCHAR"
	if col != expect {
		t.Errorf("got %v\nwant %v", col, expect)
	}
}

func TestNameWithType_Indent(t *testing.T) {
	expect := "    TEST_ID VARCHAR"
	col := NameWithType{}.Indent(Column{Name: "test_id", Type: "VARCHAR"})
	if col != expect {
		t.Errorf("got %v\nwant %v", col, expect)
	}
	col = NameWithType{}.Indent(Column{Name: "test_created_at", Type: "VARCHAR"})
	expect = "    TEST_CREATED_AT VARCHAR"
	if col != expect {
		t.Errorf("got %v\nwant %v", col, expect)
	}
}

func TestCreateTable_Generate(t *testing.T) {
	pf, err := reader.Parquet{}.Open("../example/test.parquet")
	if err != nil {
		t.Fatal(err)
	}
	s := NewDDL(pf.Schema(), value.Table).Transform()
	expect := `CREATE OR REPLACE TABLE REPLACE.ME (
    REGISTRATION_DTTM NUMBER,
    ID NUMBER,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    EMAIL VARCHAR,
    GENDER VARCHAR,
    IP_ADDRESS VARCHAR,
    CC VARCHAR,
    COUNTRY VARCHAR,
    BIRTHDATE VARCHAR,
    SALARY FLOAT,
    TITLE VARCHAR,
    COMMENTS VARCHAR
) COMMENT = 'replace me';`
	if s != expect {
		t.Errorf("got %v\nwant %v", s, expect)
	}
}