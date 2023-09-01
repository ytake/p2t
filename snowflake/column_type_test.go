package snowflake

import (
	"reflect"
	"testing"
)

func TestVarCharType_ColumnCast(t *testing.T) {
	vt := VarCharType{
		name: "message_date",
	}
	expect := [2]string{"VARCHAR", "DATE"}
	if reflect.DeepEqual(vt.ColumnCast(), expect) {
		t.Errorf("got %v\nwant %v", vt.ColumnCast(), expect)
	}
	vt = VarCharType{
		name: "message_datetime_created_at",
	}
	expect = [2]string{"VARCHAR", "TIMESTAMP"}
	if reflect.DeepEqual(vt.ColumnCast(), expect) {
		t.Errorf("got %v\nwant %v", vt.ColumnCast(), expect)
	}
	vt = VarCharType{
		name: "message_datetime_created",
	}
	if reflect.DeepEqual(vt.ColumnCast(), expect) {
		t.Errorf("got %v\nwant %v", vt.ColumnCast(), expect)
	}
	vt = VarCharType{
		name: "message",
	}
	expect = [2]string{"VARCHAR"}
	if reflect.DeepEqual(vt.ColumnCast(), expect) {
		t.Errorf("got %v\nwant %v", vt.ColumnCast(), expect)
	}
}

func TestBinaryType_ColumnCast(t *testing.T) {
	bt := BinaryType{
		name: "message",
	}
	expect := [1]string{"BINARY"}
	if reflect.DeepEqual(bt.ColumnCast(), expect) {
		t.Errorf("got %v\nwant %v", bt.ColumnCast(), expect)
	}
}

func TestBooleanType_ColumnCast(t *testing.T) {
	bt := BooleanType{
		name: "message",
	}
	expect := [1]string{"BOOLEAN"}
	if reflect.DeepEqual(bt.ColumnCast(), expect) {
		t.Errorf("got %v\nwant %v", bt.ColumnCast(), expect)
	}
}
func TestDateType_ColumnCast(t *testing.T) {
	dt := DateType{
		name: "message",
	}
	expect := [1]string{"DATE"}
	if reflect.DeepEqual(dt.ColumnCast(), expect) {
		t.Errorf("got %v\nwant %v", dt.ColumnCast(), expect)
	}
}

func TestTimestampType_ColumnCast(t *testing.T) {
	tt := TimestampType{
		name: "message",
	}
	expect := [1]string{"TIMESTAMP"}
	if reflect.DeepEqual(tt.ColumnCast(), expect) {
		t.Errorf("got %v\nwant %v", tt.ColumnCast(), expect)
	}
}

func TestVariantType_ColumnCast(t *testing.T) {
	vt := VariantType{
		name: "message",
	}
	expect := [1]string{"VARIANT"}
	if reflect.DeepEqual(vt.ColumnCast(), expect) {
		t.Errorf("got %v\nwant %v", vt.ColumnCast(), expect)
	}
}

func TestColumn_UpperType(t *testing.T) {
	c := Column{Name: "test", Type: "varchar"}.UpperType()
	if c != "VARCHAR" {
		t.Errorf("got %v\nwant %v", c, "VARCHAR")
	}
	c = Column{Name: "test", Type: "binary"}.UpperType()
	if c != "BINARY" {
		t.Errorf("got %v\nwant %v", c, "BINARY")
	}
	c = Column{Name: "test", Type: "boolean"}.UpperType()
	if c != "BOOLEAN" {
		t.Errorf("got %v\nwant %v", c, "BOOLEAN")
	}
	c = Column{Name: "test", Type: "date"}.UpperType()
	if c != "DATE" {
		t.Errorf("got %v\nwant %v", c, "DATE")
	}
}

func TestColumn_UpperName(t *testing.T) {
	c := Column{Name: "test", Type: "varchar"}.UpperName()
	if c != "TEST" {
		t.Errorf("got %v\nwant %v", c, "TEST")
	}
	c = Column{Name: "test", Type: "binary"}.UpperName()
	if c != "TEST" {
		t.Errorf("got %v\nwant %v", c, "TEST")
	}
	c = Column{Name: "test", Type: "boolean"}.UpperName()
	if c != "TEST" {
		t.Errorf("got %v\nwant %v", c, "TEST")
	}
	c = Column{Name: "test", Type: "date"}.UpperName()
	if c != "TEST" {
		t.Errorf("got %v\nwant %v", c, "TEST")
	}
}

func TestColumn_DetectTypeName(t *testing.T) {
	c := Column{Name: "test", Type: "varchar"}.DetectTypeName()
	if _, ok := c.(VarCharType); !ok {
		t.Errorf("got %v\nwant %v", c, "VarCharType")
	}
	c = Column{Name: "test", Type: "BYTE_ARRAY"}.DetectTypeName()
	if _, ok := c.(VariantType); !ok {
		t.Errorf("got %v\nwant %v", c, "VariantType")
	}
	c = Column{Name: "test", Type: "boolean"}.DetectTypeName()
	if _, ok := c.(BooleanType); !ok {
		t.Errorf("got %v\nwant %v", c, "BooleanType")
	}
	c = Column{Name: "test", Type: "date"}.DetectTypeName()
	if _, ok := c.(DateType); !ok {
		t.Errorf("got %v\nwant %v", c, "DateType")
	}
	c = Column{Name: "test", Type: "timestamp"}.DetectTypeName()
	if _, ok := c.(VarCharType); !ok {
		t.Errorf("got %v\nwant %v", c, "VarCharType")
	}
	c = Column{Name: "test", Type: "variant"}.DetectTypeName()
	if _, ok := c.(VarCharType); !ok {
		t.Errorf("got %v\nwant %v", c, "VarCharType")
	}
	c = Column{Name: "test_date", Type: "STRING"}.DetectTypeName()
	if _, ok := c.(VarCharType); !ok {
		t.Errorf("got %v\nwant %v", c, "TimestampType")
	}
	c = Column{Name: "test_datetime_created_at", Type: "STRING"}.DetectTypeName()
	if _, ok := c.(VarCharType); !ok {
		t.Errorf("got %v\nwant %v", c, "TimestampType")
	}
	c = Column{Name: "test_id", Type: "INT32"}.DetectTypeName()
	if _, ok := c.(NumberType); !ok {
		t.Errorf("got %v\nwant %v", c, "NumberType")
	}
	c = Column{Name: "test_id", Type: "INT64"}.DetectTypeName()
	if _, ok := c.(NumberType); !ok {
		t.Errorf("got %v\nwant %v", c, "NumberType")
	}
	c = Column{Name: "test_id", Type: "FLOAT"}.DetectTypeName()
	if _, ok := c.(FloatType); !ok {
		t.Errorf("got %v\nwant %v", c, "FloatType")
	}
}

func TestFloatType_ColumnCast(t *testing.T) {
	ft := FloatType{
		name: "message",
	}
	expect := [1]string{"FLOAT"}
	if reflect.DeepEqual(ft.ColumnCast(), expect) {
		t.Errorf("got %v\nwant %v", ft.ColumnCast(), expect)
	}
}

func TestNumberType_ColumnCast(t *testing.T) {
	nt := NumberType{
		name: "message",
	}
	expect := [1]string{"NUMBER"}
	if reflect.DeepEqual(nt.ColumnCast(), expect) {
		t.Errorf("got %v\nwant %v", nt.ColumnCast(), expect)
	}
}
