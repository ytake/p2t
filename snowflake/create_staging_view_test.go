package snowflake

import (
	"testing"

	"github.com/ytake/p2t/reader"
	"github.com/ytake/p2t/value"
)

func TestNameWithTypeCast_Name(t *testing.T) {
	expect := "$1:test_id::VARCHAR"
	col := NameWithTypeCast{}.Name(Column{Name: "test_id", Type: "VARCHAR"})
	if col != expect {
		t.Errorf("got %v\nwant %v", col, expect)
	}
	col = NameWithTypeCast{}.Name(Column{Name: "test_created_at", Type: "VARCHAR"})
	expect = "$1:test_created_at::VARCHAR::TIMESTAMP_NTZ(9)"
	if col != expect {
		t.Errorf("got %v\nwant %v", col, expect)
	}
}

func TestNameWithTypeCast_Indent(t *testing.T) {
	expect := "    $1:test_id::VARCHAR AS TEST_ID"
	col := NameWithTypeCast{}.Indent(Column{Name: "test_id", Type: "VARCHAR"})
	if col != expect {
		t.Errorf("got %v\nwant %v", col, expect)
	}
	col = NameWithTypeCast{}.Indent(Column{Name: "test_created_at", Type: "VARCHAR"})
	expect = "    $1:test_created_at::VARCHAR::TIMESTAMP_NTZ(9) AS TEST_CREATED_AT"
	if col != expect {
		t.Errorf("got %v\nwant %v", col, expect)
	}
}

func TestCreateView_Generate(t *testing.T) {
	pf, err := reader.Parquet{}.Open("../testdata/test.parquet")
	if err != nil {
		t.Fatal(err)
	}
	s := NewDDL(pf.Schema(), value.View).Transform()
	expect := `SELECT
    $1:registration_dttm::NUMBER(38,0) AS REGISTRATION_DTTM,
    $1:id::NUMBER(38,0) AS ID,
    $1:first_name::VARCHAR AS FIRST_NAME,
    $1:last_name::VARCHAR AS LAST_NAME,
    $1:email::VARCHAR AS EMAIL,
    $1:gender::VARCHAR AS GENDER,
    $1:ip_address::VARCHAR AS IP_ADDRESS,
    $1:cc::VARCHAR AS CC,
    $1:country::VARCHAR AS COUNTRY,
    $1:birthdate::VARCHAR::DATE AS BIRTHDATE,
    $1:salary::FLOAT AS SALARY,
    $1:title::VARCHAR AS TITLE,
    $1:comments::VARCHAR AS COMMENTS,
    METADATA$FILENAME AS FILENAME,
    METADATA$FILE_ROW_NUMBER AS FILE_ROW_NUMBER,
    METADATA$FILE_CONTENT_KEY AS FILE_CONTENT_KEY,
    METADATA$FILE_LAST_MODIFIED AS FILE_LAST_MODIFIED,
    METADATA$START_SCAN_TIME AS START_SCAN_TIME
FROM REPLACE.ME;`
	if s != expect {
		t.Errorf("got %v\nwant %v", s, expect)
	}
}
