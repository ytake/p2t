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
	expect = "TEST_CREATED_AT TIMESTAMP_NTZ(9)"
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
	expect = "    TEST_CREATED_AT TIMESTAMP_NTZ(9)"
	if col != expect {
		t.Errorf("got %v\nwant %v", col, expect)
	}
}

func TestCreateTable_Generate(t *testing.T) {
	pf, err := reader.Parquet{}.Open("../testdata/test.parquet")
	if err != nil {
		t.Fatal(err)
	}
	s := NewDDL(pf.Schema(), value.Table).Transform()
	expect := `CREATE OR REPLACE TABLE REPLACE.ME (
    REGISTRATION_DTTM NUMBER(38,0),
    ID NUMBER(38,0),
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    EMAIL VARCHAR,
    GENDER VARCHAR,
    IP_ADDRESS VARCHAR,
    CC VARCHAR,
    COUNTRY VARCHAR,
    BIRTHDATE DATE,
    SALARY FLOAT,
    TITLE VARCHAR,
    COMMENTS VARCHAR,
    FILENAME VARCHAR,
    FILE_ROW_NUMBER NUMBER(38,0),
    FILE_CONTENT_KEY VARCHAR,
    FILE_LAST_MODIFIED TIMESTAMP_NTZ(9),
    START_SCAN_TIME TIMESTAMP_LTZ(9)
) COMMENT = 'replace me';`
	if s != expect {
		t.Errorf("got %v\nwant %v", s, expect)
	}
}
