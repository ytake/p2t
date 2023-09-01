package snowflake

import (
	"fmt"
	"strings"

	"github.com/ytake/p2t/value"
)

var datetimeName = [...]string{
	"created_at",
	"updated_at",
	"deleted_at",
	"published_at",
	"expired_at",
	"start_at",
	"end_at",
	"datetime",
	"timestamp",
	"created",
	"updated",
	"deleted",
}

var floatType = [...]string{
	"FLOAT",
	"DOUBLE",
}

var intType = [...]string{
	"INT",
	"FIXED_LEN_BYTE_ARRAY",
}

var variantType = [...]string{
	"BSON",
	"JSON",
	"BYTE_ARRAY",
	"LIST",
	"MAP",
	"group",
}

var timestampType = [...]string{
	"TIME(isAdjustedToUTC",
	"TIMESTAMP(isAdjustedToUTC=",
}

type ColumnTyper interface {
	String() string
	ColumnCast() []string
}

type NumberType struct {
	name string
}

func (n NumberType) String() string {
	return "NUMBER"
}

func (n NumberType) ColumnCast() []string {
	return []string{n.String()}
}

type FloatType struct {
	name string
}

func (f FloatType) String() string {
	return "FLOAT"
}

func (f FloatType) ColumnCast() []string {
	return []string{f.String()}
}

type VarCharType struct {
	name string
}

func (v VarCharType) String() string {
	return "VARCHAR"
}

// IsDatetimeType is a method for checking date type.
func (v VarCharType) IsDatetimeType() bool {
	for _, r := range datetimeName {
		if strings.HasSuffix(v.name, r) {
			return true
		}
	}
	return false
}

func (v VarCharType) IsDateType() bool {
	if strings.HasSuffix(v.name, "date") {
		return true
	}
	return false
}

func (v VarCharType) extendedCast() []string {
	if v.IsDateType() {
		return DateType{}.ColumnCast()
	}
	if v.IsDatetimeType() {
		return TimestampType{}.ColumnCast()
	}
	return []string{}
}

func (v VarCharType) ColumnCast() []string {
	s := []string{v.String()}
	return append(s, v.extendedCast()...)
}

type BinaryType struct {
	name string
}

func (b BinaryType) String() string {
	return "BINARY"
}

func (b BinaryType) ColumnCast() []string {
	return []string{b.String()}
}

type BooleanType struct {
	name string
}

func (b BooleanType) String() string {
	return "BOOLEAN"
}

func (b BooleanType) ColumnCast() []string {
	return []string{b.String()}
}

type DateType struct {
	name string
}

func (d DateType) String() string {
	return "DATE"
}

func (d DateType) ColumnCast() []string {
	return []string{d.String()}
}

type TimestampType struct {
	name string
}

func (t TimestampType) String() string {
	return "TIMESTAMP"
}

func (t TimestampType) ColumnCast() []string {
	return []string{t.String()}
}

type VariantType struct {
	name string
}

func (v VariantType) String() string {
	return "VARIANT"
}

func (v VariantType) ColumnCast() []string {
	return []string{v.String()}
}

type ObjectType struct{}
type ArrayType struct{}
type GeographyType struct{}
type GeometryType struct{}

type CreateTable struct {
	columns []value.ColumnDefinition
}

type CreateView struct {
	columns []value.ColumnDefinition
}

const (
	ParquetPrefix = "$1"
)

// Column is a struct for column definition.
type (
	Column struct {
		Name     string
		Optional bool
		Type     string
	}
	ColumnNamer interface {
		Name(c Column) string
		Indent(c Column) string
	}
)

// UpperName is a method for getting column name.
func (c Column) UpperName() string {
	return strings.ToUpper(c.Name)
}

// UpperType is a method for getting column name.
func (c Column) UpperType() string {
	return strings.ToUpper(c.Type)
}

// Indent is a method for getting indent column name.
func (c Column) Indent() string {
	return fmt.Sprintf("    %s", c.UpperName())
}

// DetectTypeName is a method for detecting column type.
func (c Column) DetectTypeName() ColumnTyper {
	for _, v := range timestampType {
		if strings.Contains(c.UpperType(), v) {
			return TimestampType{
				name: c.Name,
			}
		}
	}
	for _, v := range intType {
		if strings.Contains(c.UpperType(), v) {
			return NumberType{
				name: c.Name,
			}
		}
	}
	for _, v := range floatType {
		if strings.Contains(c.UpperType(), v) {
			return FloatType{
				name: c.Name,
			}
		}
	}
	if strings.Contains(c.UpperType(), "BOOLEAN") {
		return BooleanType{
			name: c.Name,
		}
	}
	for _, v := range variantType {
		if strings.Contains(c.UpperType(), v) {
			return VariantType{
				name: c.Name,
			}
		}
	}
	if strings.Contains(c.UpperType(), "DATE") {
		return DateType{
			name: c.Name,
		}
	}
	return VarCharType{
		name: c.Name,
	}
}
