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
	expect = "$1:test_created_at::VARCHAR::TIMESTAMP"
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
	expect = "    $1:test_created_at::VARCHAR::TIMESTAMP AS TEST_CREATED_AT"
	if col != expect {
		t.Errorf("got %v\nwant %v", col, expect)
	}
}

func TestCreateView_Generate(t *testing.T) {
	pf, err := reader.Parquet{}.Open("../example/test.parquet")
	if err != nil {
		t.Fatal(err)
	}
	s := NewDDL(pf.Schema(), value.View).Transform()
	expect := `CREATE OR REPLACE VIEW REPLACE.ME (
    REGISTRATION_DTTM,
    ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    GENDER,
    IP_ADDRESS,
    CC,
    COUNTRY,
    BIRTHDATE,
    SALARY,
    TITLE,
    COMMENTS
) AS 
SELECT
    $1:registration_dttm::NUMBER AS REGISTRATION_DTTM,
    $1:id::NUMBER AS ID,
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
    $1:comments::VARCHAR AS COMMENTS
FROM REPLACE.ME;`
	if s != expect {
		t.Errorf("got %v\nwant %v", s, expect)
	}
}
