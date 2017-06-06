package query

import (
	"io"
	"regexp"

	"github.com/influxdata/influxdb/influxql"
)

type ShardGroup interface {
	MeasurementsByRegex(re *regexp.Regexp) []string
	FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	MapType(measurement, field string) influxql.DataType
	CreateIterator(measurement string, opt influxql.IteratorOptions) (influxql.Iterator, error)
}

type ShardMapper interface {
	MapShards(m *influxql.Measurement, opt *influxql.SelectOptions) (Database, error)
}

// compiledSource is a source that links together a variable reference with a source.
type compiledSource interface {
	link(m ShardMapper) (storage, error)
}

type Database interface {
	CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error)
	FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	MapType(field string) influxql.DataType
	io.Closer
}

// storage is an abstraction over the storage layer.
type storage interface {
	FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	MapType(field string) influxql.DataType
	io.Closer
	resolve(ref *influxql.VarRef, out *WriteEdge)
}

type measurement struct {
	stmt   *compiledStatement
	source *influxql.Measurement
}

func (m *measurement) link(shardMapper ShardMapper) (storage, error) {
	opt := influxql.SelectOptions{
		MinTime: m.stmt.TimeRange.Min,
		MaxTime: m.stmt.TimeRange.Max,
	}
	shard, err := shardMapper.MapShards(m.source, &opt)
	if err != nil {
		return nil, err
	}
	return &storageEngine{
		stmt:  m.stmt,
		shard: shard,
	}, nil
}

type storageEngine struct {
	stmt  *compiledStatement
	shard Database
}

func (s *storageEngine) FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	return s.shard.FieldDimensions()
}

func (s *storageEngine) MapType(field string) influxql.DataType {
	return s.shard.MapType(field)
}

func (s *storageEngine) resolve(ref *influxql.VarRef, out *WriteEdge) {
	ic := &IteratorCreator{
		Expr:            ref,
		AuxiliaryFields: s.stmt.AuxiliaryFields,
		Database:        s.shard,
		Dimensions:      s.stmt.Dimensions,
		Tags:            s.stmt.Tags,
		Interval:        s.stmt.Interval,
		TimeRange:       s.stmt.TimeRange,
		Ascending:       s.stmt.Ascending,
		Output:          out,
	}
	out.Node = ic
}

func (s *storageEngine) Close() error {
	return s.shard.Close()
}

type subquery struct {
	stmt      *compiledStatement
	auxFields *AuxiliaryFields
}

func (s *subquery) link(m ShardMapper) (storage, error) {
	// Link the subquery statement and store the linked fields.
	fields, columns, err := s.stmt.link(m)
	if err != nil {
		return nil, err
	}

	// Retrieve the dimensions for this statement.
	dimensions := make([]string, 0, len(s.stmt.Tags))
	for d := range s.stmt.Tags {
		dimensions = append(dimensions, d)
	}

	// Pack all of the output fields into the iterator mapper.
	mapper := &IteratorMapper{AuxiliaryFields: s.auxFields}
	mapper.InputNodes = make([]*ReadEdge, len(fields))
	for i, f := range fields {
		mapper.InputNodes[i] = f.Output
		f.Output.Node = mapper
	}

	return &subqueryEngine{
		fields:     fields,
		columns:    columns[1:],
		dimensions: dimensions,
		mapper:     mapper,
	}, nil
}

type subqueryEngine struct {
	fields     []*compiledField
	columns    []string
	dimensions []string
	mapper     *IteratorMapper
}

func (s *subqueryEngine) FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	// Iterate over each of the fields and retrieve the type from the graph.
	// Since linking has already been performed, everything should have types.
	for i, f := range s.fields {
		fields[s.columns[i]] = f.Output.Input.Node.Type()
	}

	// Copy the dimensions of the subquery into the dimensions.
	for _, d := range s.dimensions {
		dimensions[d] = struct{}{}
	}
	return fields, dimensions, nil
}

func (s *subqueryEngine) MapType(field string) influxql.DataType {
	// Find the appropriate column for this field.
	for i, name := range s.columns {
		if name == field {
			return s.fields[i].Output.Input.Node.Type()
		}
	}
	return influxql.Unknown
}

func (s *subqueryEngine) Close() error {
	return nil
}

func (s *subqueryEngine) resolve(ref *influxql.VarRef, out *WriteEdge) {
	if ref == nil {
		s.mapper.Aux(out)
		return
	}

	clone := *ref
	field := &AuxiliaryField{Ref: &clone, Output: out}
	out.Node = field
	out, field.Input = AddEdge(s.mapper, field)

	// Search for a field matching the reference.
	for i, name := range s.columns {
		if name == ref.Val {
			// Match this output edge with this field.
			s.mapper.Field(i, out)
			field.Ref.Type = s.fields[i].Output.Input.Node.Type()
			return
		}
	}

	// Search for a tag matching the reference.
	for _, d := range s.dimensions {
		if d == ref.Val {
			s.mapper.Tag(d, out)
			field.Ref.Type = influxql.Tag
			return
		}
	}

	// If all else fails, attach it to a nil reference.
	n := &Nil{Output: out}
	out.Node = n
}

type storageEngines []storage

func (a storageEngines) FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	for _, s := range a {
		f, d, err := s.FieldDimensions()
		if err != nil {
			return nil, nil, err
		}

		for k, typ := range f {
			if _, ok := fields[k]; typ != influxql.Unknown && (!ok || typ < fields[k]) {
				fields[k] = typ
			}
		}
		for k := range d {
			dimensions[k] = struct{}{}
		}
	}
	return fields, dimensions, nil
}

func (a storageEngines) MapType(field string) influxql.DataType {
	var typ influxql.DataType
	for _, s := range a {
		if t := s.MapType(field); typ.LessThan(t) {
			typ = t
		}
	}
	return typ
}

func (a storageEngines) resolve(ref *influxql.VarRef, out *WriteEdge) {
	merge := &Merge{Output: out}
	out.Node = merge
	for _, s := range a {
		out := merge.AddInput(nil)
		s.resolve(ref, out)
	}
}

func (a storageEngines) Close() error {
	for _, s := range a {
		s.Close()
	}
	return nil
}
