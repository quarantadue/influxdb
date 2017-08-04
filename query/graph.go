package query

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"sort"

	"github.com/influxdata/influxdb/influxql"
)

// WriteEdge is the end of the edge that is written to by the Node.
type WriteEdge struct {
	// Node is the node that creates an Iterator and sends it to this edge.
	// This should always be set to a value.
	Node Node

	// Output is the output end of the edge. This should always be set.
	Output *ReadEdge

	itr   influxql.Iterator
	ready bool
	mu    sync.RWMutex
}

// SetIterator marks this Edge as ready and sets the Iterator as the returned
// iterator. If the Edge has already been set, this panics. The result can be
// retrieved from the output edge.
func (e *WriteEdge) SetIterator(itr influxql.Iterator) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.ready {
		panic("unable to call SetIterator on the same node twice")
	}
	e.itr = itr
	e.ready = true
}

type WrapIteratorFn func(input influxql.Iterator) influxql.Iterator

func (e *WriteEdge) WrapIterator(fn WrapIteratorFn) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.ready {
		panic(fmt.Sprintf("attempted to wrap an iterator from an edge before it was ready: %T", e.Node))
	}
	e.itr = fn(e.itr)
}

// Attach attaches this WriteEdge to a Node and returns the WriteEdge so it
// can be assigned to the Node's output.
func (e *WriteEdge) Attach(n Node) *WriteEdge {
	e.Node = n
	return e
}

// Chain attaches this Node to the WriteEdge and creates a new ReadEdge that
// is attached to the same node. It returns a new WriteEdge to replace
// the one.
func (e *WriteEdge) Chain(n Node) (*WriteEdge, *ReadEdge) {
	e.Node = n
	return AddEdge(nil, n)
}

// ReadEdge is the end of the edge that reads from the Iterator.
type ReadEdge struct {
	// Node is the node that will read the Iterator from this edge.
	// This may be nil if there is no Node that will read this edge.
	Node Node

	// Input is the input end of the edge. This should always be set.
	Input *WriteEdge
}

// Iterator returns the Iterator created for this Node by the WriteEdge.
// If the WriteEdge is not ready, this function will panic.
func (e *ReadEdge) Iterator() influxql.Iterator {
	e.Input.mu.RLock()
	if !e.Input.ready {
		e.Input.mu.RUnlock()
		panic(fmt.Sprintf("attempted to retrieve an iterator from an edge before it was ready: %T", e.Input.Node))
	}
	itr := e.Input.itr
	e.Input.mu.RUnlock()
	return itr
}

// Ready returns whether this ReadEdge is ready to be read from. This edge
// will be ready after the attached WriteEdge has called SetIterator().
func (e *ReadEdge) Ready() (ready bool) {
	e.Input.mu.RLock()
	ready = e.Input.ready
	e.Input.mu.RUnlock()
	return ready
}

// Insert splits the current edge and inserts a Node into the middle.
// It then returns the newly created ReadEdge that points to the inserted
// Node and the newly created WriteEdge that the Node should use to send its
// results.
func (e *ReadEdge) Insert(n Node) (*ReadEdge, *WriteEdge) {
	// Create a new ReadEdge. The input should be the current WriteEdge for
	// this node.
	out := &ReadEdge{Node: n, Input: e.Input}
	// Reset the Input so it points to the newly created output as its input.
	e.Input.Output = out
	// Redirect this ReadEdge's input to a new input edge.
	e.Input = &WriteEdge{Node: n, Output: e}
	// Return the newly created edges so they can be stored with the newly
	// inserted Node.
	return out, e.Input
}

// NewEdge creates a new edge where both sides are unattached.
func NewEdge() (*WriteEdge, *ReadEdge) {
	input := &WriteEdge{}
	output := &ReadEdge{}
	input.Output, output.Input = output, input
	return input, output
}

// NewReadEdge creates a new edge with the ReadEdge attached to the Node
// and an unattached WriteEdge.
func NewReadEdge(n Node) (*WriteEdge, *ReadEdge) {
	input, output := NewEdge()
	output.Node = n
	return input, output
}

// AddEdge creates a new edge between two nodes.
func AddEdge(in, out Node) (*WriteEdge, *ReadEdge) {
	input := &WriteEdge{Node: in}
	output := &ReadEdge{Node: out}
	input.Output, output.Input = output, input
	return input, output
}

type Node interface {
	// Description returns a brief description about what this node does.  This
	// should include details that describe what the node will do based on the
	// current configuration of the node.
	Description() string

	// Inputs returns the Edges that produce Iterators that will be consumed by
	// this Node.
	Inputs() []*ReadEdge

	// Outputs returns the Edges that will receive an Iterator from this Node.
	Outputs() []*WriteEdge

	// Execute executes the Node and transmits the created Iterators to the
	// output edges.
	Execute() error

	// Type returns the type output for this node if it is known. Typically,
	// type information is available after linking, but it may be known
	// before that.
	Type() influxql.DataType
}

type OptimizableNode interface {
	Node
	Optimize()
}

// RestrictedTypeInputNode is implemented by Nodes that accept a restricted
// set of type options.
type RestrictedTypeInputNode interface {
	Node

	// ValidateInputTypes validates the inputs for this Node and
	// returns an appropriate error if the input type is not valid.
	ValidateInputTypes() error
}

// AllInputsReady determines if all of the input edges for a node are ready.
func AllInputsReady(n Node) bool {
	inputs := n.Inputs()
	if len(inputs) == 0 {
		return true
	}

	for _, input := range inputs {
		if !input.Ready() {
			return false
		}
	}
	return true
}

var _ Node = &IteratorCreator{}

type IteratorCreator struct {
	Expr            *influxql.VarRef
	AuxiliaryFields *AuxiliaryFields
	Database        Database
	Dimensions      []string
	Tags            map[string]struct{}
	Interval        influxql.Interval
	Ordered         bool
	TimeRange       TimeRange
	Ascending       bool
	Output          *WriteEdge
}

func (ic *IteratorCreator) Description() string {
	var buf bytes.Buffer
	buf.WriteString("create iterator")
	if ic.Expr != nil {
		fmt.Fprintf(&buf, " for %s", ic.Expr)
	}
	if ic.AuxiliaryFields != nil {
		names := make([]string, 0, len(ic.AuxiliaryFields.Aux))
		for _, name := range ic.AuxiliaryFields.Aux {
			names = append(names, name.String())
		}
		fmt.Fprintf(&buf, " [aux: %s]", strings.Join(names, ", "))
	}
	if ic.Tags != nil {
		tags := make([]string, 0, len(ic.Tags))
		for d := range ic.Tags {
			tags = append(tags, d)
		}
		sort.Strings(tags)
		fmt.Fprintf(&buf, " group by %s", strings.Join(tags, ", "))
	}
	if !ic.Interval.IsZero() {
		fmt.Fprintf(&buf, " interval(%s)", ic.Interval.Duration)
	}
	fmt.Fprintf(&buf, " from %s to %s", ic.TimeRange.Min, ic.TimeRange.Max)
	return buf.String()
}

func (ic *IteratorCreator) Inputs() []*ReadEdge { return nil }
func (ic *IteratorCreator) Outputs() []*WriteEdge {
	if ic.Output != nil {
		return []*WriteEdge{ic.Output}
	}
	return nil
}

func (ic *IteratorCreator) Execute() error {
	var auxFields []influxql.VarRef
	if ic.AuxiliaryFields != nil {
		auxFields = ic.AuxiliaryFields.Aux
	}
	opt := influxql.IteratorOptions{
		Expr:       ic.Expr,
		Dimensions: ic.Dimensions,
		GroupBy:    ic.Tags,
		Interval:   ic.Interval,
		Ordered:    ic.Ordered,
		Aux:        auxFields,
		StartTime:  ic.TimeRange.Min.UnixNano(),
		EndTime:    ic.TimeRange.Max.UnixNano(),
		Ascending:  ic.Ascending,
	}
	itr, err := ic.Database.CreateIterator(opt)
	if err != nil {
		return err
	}
	ic.Output.SetIterator(itr)
	return nil
}

func (ic *IteratorCreator) Type() influxql.DataType {
	if ic.Expr != nil {
		return ic.Database.MapType(ic.Expr.Val)
	}
	return influxql.Unknown
}

var _ Node = &Merge{}

type Merge struct {
	InputNodes []*ReadEdge
	Output     *WriteEdge
}

func (m *Merge) Description() string {
	return fmt.Sprintf("merge %d nodes", len(m.InputNodes))
}

func (m *Merge) AddInput(n Node) *WriteEdge {
	in, out := AddEdge(n, m)
	m.InputNodes = append(m.InputNodes, out)
	return in
}

func (m *Merge) Inputs() []*ReadEdge   { return m.InputNodes }
func (m *Merge) Outputs() []*WriteEdge { return []*WriteEdge{m.Output} }

func (m *Merge) Execute() error {
	if len(m.InputNodes) == 0 {
		m.Output.SetIterator(nil)
		return nil
	} else if len(m.InputNodes) == 1 {
		m.Output.SetIterator(m.InputNodes[0].Iterator())
		return nil
	}

	inputs := make([]influxql.Iterator, len(m.InputNodes))
	for i, input := range m.InputNodes {
		inputs[i] = input.Iterator()
	}
	itr := influxql.NewSortedMergeIterator(inputs, influxql.IteratorOptions{Ascending: true})
	m.Output.SetIterator(itr)
	return nil
}

func (m *Merge) Type() influxql.DataType {
	var typ influxql.DataType
	for _, input := range m.Inputs() {
		if n := input.Input.Node; n != nil {
			if t := n.Type(); typ.LessThan(t) {
				typ = t
			}
		}
	}
	return typ
}

func (m *Merge) Optimize() {
	// Nothing to optimize if we are not pointed at anything.
	if m.Output.Output.Node == nil {
		return
	}

	switch node := m.Output.Output.Node.(type) {
	case *FunctionCall:
		// If our output node is a function, check if it is one of the ones we can
		// do as a partial aggregate.
		switch node.Name {
		case "min", "max", "sum", "first", "last", "mean", "count":
			// Pass through.
		default:
			return
		}

		// Create a new function call and insert it at the end of every
		// input to the merge node.
		for _, input := range m.InputNodes {
			call := &FunctionCall{
				Name:       node.Name,
				Dimensions: node.Dimensions,
				GroupBy:    node.GroupBy,
				Interval:   node.Interval,
				Ascending:  node.Ascending,
			}
			call.Input, call.Output = input.Insert(call)
		}

		// If the function call was count(), modify it so it is now sum().
		if node.Name == "count" {
			node.Name = "sum"
		}
	}
}

var _ Node = &FunctionCall{}

type FunctionCall struct {
	Name       string
	Dimensions []string
	GroupBy    map[string]struct{}
	Interval   influxql.Interval
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (c *FunctionCall) Description() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s()", c.Name)
	if len(c.GroupBy) > 0 {
		tags := make([]string, 0, len(c.GroupBy))
		for d := range c.GroupBy {
			tags = append(tags, d)
		}
		sort.Strings(tags)
		fmt.Fprintf(&buf, " group by %s", strings.Join(tags, ", "))
	}
	return buf.String()
}

func (c *FunctionCall) Inputs() []*ReadEdge   { return []*ReadEdge{c.Input} }
func (c *FunctionCall) Outputs() []*WriteEdge { return []*WriteEdge{c.Output} }

func (c *FunctionCall) Execute() error {
	input := c.Input.Iterator()
	if input == nil {
		c.Output.SetIterator(input)
		return nil
	}

	call := &influxql.Call{Name: c.Name}
	opt := influxql.IteratorOptions{
		Expr:       call,
		Dimensions: c.Dimensions,
		GroupBy:    c.GroupBy,
		Interval:   c.Interval,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  c.Ascending,
	}
	itr, err := influxql.NewCallIterator(input, opt)
	if err != nil {
		return err
	}
	c.Output.SetIterator(itr)
	return nil
}

func (c *FunctionCall) Type() influxql.DataType {
	switch c.Name {
	case "mean":
		return influxql.Float
	case "count":
		return influxql.Integer
	default:
		if n := c.Input.Input.Node; n != nil {
			return n.Type()
		}
		return influxql.Unknown
	}
}

func (c *FunctionCall) ValidateInputTypes() error {
	n := c.Input.Input.Node
	if n == nil {
		return nil
	}

	typ := n.Type()
	if typ == influxql.Unknown {
		return nil
	}

	switch c.Name {
	case "min", "max", "sum", "mean":
		if typ != influxql.Float && typ != influxql.Integer {
			return fmt.Errorf("cannot use type %s in argument to %s", typ, c.Name)
		}
	}
	return nil
}

type Median struct {
	Dimensions []string
	GroupBy    map[string]struct{}
	Interval   influxql.Interval
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (m *Median) Description() string {
	return "median()"
}

func (m *Median) Inputs() []*ReadEdge   { return []*ReadEdge{m.Input} }
func (m *Median) Outputs() []*WriteEdge { return []*WriteEdge{m.Output} }

func (m *Median) Execute() error {
	input := m.Input.Iterator()
	if input == nil {
		m.Output.SetIterator(input)
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: m.Dimensions,
		GroupBy:    m.GroupBy,
		Interval:   m.Interval,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  m.Ascending,
	}
	itr, err := influxql.NewMedianIterator(input, opt)
	if err != nil {
		return err
	}
	m.Output.SetIterator(itr)
	return nil
}

func (m *Median) Type() influxql.DataType {
	return influxql.Float
}

func (m *Median) ValidateInputTypes() error {
	n := m.Input.Input.Node
	if n == nil {
		return nil
	}

	typ := n.Type()
	if typ == influxql.Unknown {
		return nil
	} else if typ != influxql.Float && typ != influxql.Integer {
		return fmt.Errorf("cannot use type %s in argument to median", typ)
	}
	return nil
}

type Mode struct {
	Input  *ReadEdge
	Output *WriteEdge
}

func (m *Mode) Description() string {
	return "mode()"
}

func (m *Mode) Inputs() []*ReadEdge   { return []*ReadEdge{m.Input} }
func (m *Mode) Outputs() []*WriteEdge { return []*WriteEdge{m.Output} }

func (m *Mode) Execute() error {
	return errors.New("unimplemented")
}

func (m *Mode) Type() influxql.DataType {
	if n := m.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type Stddev struct {
	Input  *ReadEdge
	Output *WriteEdge
}

func (s *Stddev) Description() string {
	return "stddev()"
}

func (s *Stddev) Inputs() []*ReadEdge   { return []*ReadEdge{s.Input} }
func (s *Stddev) Outputs() []*WriteEdge { return []*WriteEdge{s.Output} }

func (s *Stddev) Execute() error {
	return errors.New("unimplemented")
}

func (s *Stddev) Type() influxql.DataType {
	if n := s.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type Spread struct {
	Input  *ReadEdge
	Output *WriteEdge
}

func (s *Spread) Description() string {
	return "spread()"
}

func (s *Spread) Inputs() []*ReadEdge   { return []*ReadEdge{s.Input} }
func (s *Spread) Outputs() []*WriteEdge { return []*WriteEdge{s.Output} }

func (s *Spread) Execute() error {
	return errors.New("unimplemented")
}

func (s *Spread) Type() influxql.DataType {
	if n := s.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type Percentile struct {
	Number float64
	Input  *ReadEdge
	Output *WriteEdge
}

func (p *Percentile) Description() string {
	return fmt.Sprintf("percentile(%2.f)", p.Number)
}

func (p *Percentile) Inputs() []*ReadEdge   { return []*ReadEdge{p.Input} }
func (p *Percentile) Outputs() []*WriteEdge { return []*WriteEdge{p.Output} }

func (p *Percentile) Execute() error {
	return errors.New("unimplemented")
}

func (p *Percentile) Type() influxql.DataType {
	if n := p.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type Sample struct {
	N      int
	Input  *ReadEdge
	Output *WriteEdge
}

func (s *Sample) Description() string {
	return fmt.Sprintf("sample(%d)", s.N)
}

func (s *Sample) Inputs() []*ReadEdge   { return []*ReadEdge{s.Input} }
func (s *Sample) Outputs() []*WriteEdge { return []*WriteEdge{s.Output} }

func (s *Sample) Execute() error {
	return errors.New("unimplemented")
}

func (s *Sample) Type() influxql.DataType {
	if n := s.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type Derivative struct {
	Duration      time.Duration
	IsNonNegative bool
	Input         *ReadEdge
	Output        *WriteEdge
}

func (d *Derivative) Description() string {
	if d.IsNonNegative {
		return fmt.Sprintf("non_negative_derivative(%s)", influxql.FormatDuration(d.Duration))
	}
	return fmt.Sprintf("derivative(%s)", influxql.FormatDuration(d.Duration))
}

func (d *Derivative) Inputs() []*ReadEdge   { return []*ReadEdge{d.Input} }
func (d *Derivative) Outputs() []*WriteEdge { return []*WriteEdge{d.Output} }

func (d *Derivative) Execute() error {
	return errors.New("unimplemented")
}

func (d *Derivative) Type() influxql.DataType {
	return influxql.Float
}

type Elapsed struct {
	Duration time.Duration
	Input    *ReadEdge
	Output   *WriteEdge
}

func (e *Elapsed) Description() string {
	return fmt.Sprintf("elapsed(%s)", influxql.FormatDuration(e.Duration))
}

func (e *Elapsed) Inputs() []*ReadEdge   { return []*ReadEdge{e.Input} }
func (e *Elapsed) Outputs() []*WriteEdge { return []*WriteEdge{e.Output} }

func (e *Elapsed) Execute() error {
	return errors.New("unimplemented")
}

func (e *Elapsed) Type() influxql.DataType {
	return influxql.Integer
}

type Difference struct {
	IsNonNegative bool
	Input         *ReadEdge
	Output        *WriteEdge
}

func (d *Difference) Description() string {
	if d.IsNonNegative {
		return "non_negative_difference()"
	}
	return "difference()"
}

func (d *Difference) Inputs() []*ReadEdge   { return []*ReadEdge{d.Input} }
func (d *Difference) Outputs() []*WriteEdge { return []*WriteEdge{d.Output} }

func (d *Difference) Execute() error {
	return errors.New("unimplemented")
}

func (d *Difference) Type() influxql.DataType {
	if n := d.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type MovingAverage struct {
	WindowSize int
	Input      *ReadEdge
	Output     *WriteEdge
}

func (m *MovingAverage) Description() string {
	return fmt.Sprintf("moving_average(%d)", m.WindowSize)
}

func (m *MovingAverage) Inputs() []*ReadEdge   { return []*ReadEdge{m.Input} }
func (m *MovingAverage) Outputs() []*WriteEdge { return []*WriteEdge{m.Output} }

func (m *MovingAverage) Execute() error {
	return errors.New("unimplemented")
}

func (m *MovingAverage) Type() influxql.DataType {
	return influxql.Float
}

type CumulativeSum struct {
	Input  *ReadEdge
	Output *WriteEdge
}

func (c *CumulativeSum) Description() string {
	return "cumulative_sum()"
}

func (c *CumulativeSum) Inputs() []*ReadEdge   { return []*ReadEdge{c.Input} }
func (c *CumulativeSum) Outputs() []*WriteEdge { return []*WriteEdge{c.Output} }

func (c *CumulativeSum) Execute() error {
	return errors.New("unimplemented")
}

func (c *CumulativeSum) Type() influxql.DataType {
	if n := c.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type Integral struct {
	Duration time.Duration
	Input    *ReadEdge
	Output   *WriteEdge
}

func (i *Integral) Description() string {
	return fmt.Sprintf("integral(%s)", influxql.FormatDuration(i.Duration))
}

func (i *Integral) Inputs() []*ReadEdge   { return []*ReadEdge{i.Input} }
func (i *Integral) Outputs() []*WriteEdge { return []*WriteEdge{i.Output} }

func (i *Integral) Execute() error {
	return errors.New("unimplemented")
}

func (i *Integral) Type() influxql.DataType {
	return influxql.Float
}

type HoltWinters struct {
	N, S    int
	WithFit bool
	Input   *ReadEdge
	Output  *WriteEdge
}

func (hw *HoltWinters) Description() string {
	if hw.WithFit {
		return fmt.Sprintf("holt_winters_with_fit(%d, %d)", hw.N, hw.S)
	}
	return fmt.Sprintf("holt_winters(%d, %d)", hw.N, hw.S)
}

func (hw *HoltWinters) Inputs() []*ReadEdge   { return []*ReadEdge{hw.Input} }
func (hw *HoltWinters) Outputs() []*WriteEdge { return []*WriteEdge{hw.Output} }

func (hw *HoltWinters) Execute() error {
	return errors.New("unimplemented")
}

func (hw *HoltWinters) Type() influxql.DataType {
	return influxql.Float
}

type Distinct struct {
	Dimensions []string
	GroupBy    map[string]struct{}
	Interval   influxql.Interval
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (d *Distinct) Description() string {
	return "find distinct values"
}

func (d *Distinct) Inputs() []*ReadEdge   { return []*ReadEdge{d.Input} }
func (d *Distinct) Outputs() []*WriteEdge { return []*WriteEdge{d.Output} }

func (d *Distinct) Execute() error {
	opt := influxql.IteratorOptions{
		Dimensions: d.Dimensions,
		GroupBy:    d.GroupBy,
		Interval:   d.Interval,
		Ascending:  d.Ascending,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
	}
	itr, err := influxql.NewDistinctIterator(d.Input.Iterator(), opt)
	if err != nil {
		return err
	}
	d.Output.SetIterator(itr)
	return nil
}

func (d *Distinct) Type() influxql.DataType {
	if n := d.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type TopBottomSelector struct {
	Name       string
	Limit      int
	Dimensions []string
	Interval   influxql.Interval
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (s *TopBottomSelector) Description() string {
	return fmt.Sprintf("%s(%d)", s.Name, s.Limit)
}

func (s *TopBottomSelector) Inputs() []*ReadEdge   { return []*ReadEdge{s.Input} }
func (s *TopBottomSelector) Outputs() []*WriteEdge { return []*WriteEdge{s.Output} }

func (s *TopBottomSelector) Execute() error {
	input := s.Input.Iterator()
	if input == nil {
		s.Output.SetIterator(input)
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: s.Dimensions,
		Interval:   s.Interval,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ordered:    true,
		Ascending:  s.Ascending,
	}
	var itr influxql.Iterator
	var err error
	if s.Name == "top" {
		itr, err = influxql.NewTopIterator(input, opt, s.Limit, false)
	} else {
		itr, err = influxql.NewBottomIterator(input, opt, s.Limit, false)
	}
	if err != nil {
		return err
	}
	s.Output.SetIterator(itr)
	return nil
}

func (s *TopBottomSelector) Type() influxql.DataType {
	if n := s.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type AuxiliaryFields struct {
	Aux        []influxql.VarRef
	Dimensions []string
	Input      *ReadEdge
	Output     *WriteEdge
	outputs    []*WriteEdge
	refs       []*influxql.VarRef
}

func (c *AuxiliaryFields) Description() string {
	return "access auxiliary fields"
}

func (c *AuxiliaryFields) Inputs() []*ReadEdge { return []*ReadEdge{c.Input} }
func (c *AuxiliaryFields) Outputs() []*WriteEdge {
	if c.Output != nil {
		outputs := make([]*WriteEdge, 0, len(c.outputs)+1)
		outputs = append(outputs, c.Output)
		outputs = append(outputs, c.outputs...)
		return outputs
	} else {
		return c.outputs
	}
}

func (c *AuxiliaryFields) Execute() error {
	opt := influxql.IteratorOptions{
		Dimensions: c.Dimensions,
		Aux:        c.Aux,
	}
	aitr := influxql.NewAuxIterator(c.Input.Iterator(), opt)
	for i, ref := range c.refs {
		itr := aitr.Iterator(ref.Val, ref.Type)
		c.outputs[i].SetIterator(itr)
	}
	if c.Output != nil {
		c.Output.SetIterator(aitr)
		aitr.Start()
	} else {
		aitr.Background()
	}
	return nil
}

func (c *AuxiliaryFields) Type() influxql.DataType {
	if n := c.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

// Iterator registers an auxiliary field to be sent to the passed in WriteEdge
// and configures that WriteEdge with the AuxiliaryFields as its Node.
func (c *AuxiliaryFields) Iterator(ref *influxql.VarRef, out *WriteEdge) {
	field := &AuxiliaryField{Ref: ref, Output: out}
	out.Node = field

	out, field.Input = AddEdge(c, field)
	c.outputs = append(c.outputs, out)

	// Attempt to find an existing variable that matches this one to avoid
	// duplicating the same variable reference in the auxiliary fields.
	for idx := range c.Aux {
		v := &c.Aux[idx]
		if *v == *ref {
			c.refs = append(c.refs, v)
			return
		}
	}

	// Register a new auxiliary field and take a reference to it.
	c.Aux = append(c.Aux, *ref)
	c.refs = append(c.refs, &c.Aux[len(c.Aux)-1])
}

var _ Node = &IteratorMapper{}

type IteratorMapper struct {
	AuxiliaryFields *AuxiliaryFields
	InputNodes      []*ReadEdge
	outputs         []*WriteEdge
}

func (m *IteratorMapper) Description() string {
	return "map the results of the subquery"
}

func (m *IteratorMapper) Inputs() []*ReadEdge   { return m.InputNodes }
func (m *IteratorMapper) Outputs() []*WriteEdge { return m.outputs }

func (m *IteratorMapper) Execute() error {
	for _, output := range m.outputs {
		output.SetIterator(nil)
	}
	return nil
}

func (m *IteratorMapper) Type() influxql.DataType {
	// This iterator does not produce any direct outputs, but instead
	// reads through them and duplexes the results to its outputs.
	// So it functionally has no type for the output since it has no output.
	return influxql.Unknown
}

// Field registers an iterator to read from the field at the index.
func (m *IteratorMapper) Field(index int, out *WriteEdge) {
	m.outputs = append(m.outputs, out)
	out.Node = m
}

// Tag registers an iterator to read from the tag with the given name.
func (m *IteratorMapper) Tag(name string, out *WriteEdge) {
	m.outputs = append(m.outputs, out)
	out.Node = m
}

// Aux registers an iterator that has no driver and just reads the
// auxiliary fields.
func (m *IteratorMapper) Aux(out *WriteEdge) {
	m.outputs = append(m.outputs, out)
	out.Node = m
}

var _ Node = &AuxiliaryField{}

type AuxiliaryField struct {
	Ref    *influxql.VarRef
	Input  *ReadEdge
	Output *WriteEdge
}

func (f *AuxiliaryField) Description() string {
	return f.Ref.String()
}

func (f *AuxiliaryField) Inputs() []*ReadEdge {
	return []*ReadEdge{f.Input}
}

func (f *AuxiliaryField) Outputs() []*WriteEdge {
	return []*WriteEdge{f.Output}
}

func (f *AuxiliaryField) Execute() error {
	f.Output.SetIterator(f.Input.Iterator())
	return nil
}

func (f *AuxiliaryField) Type() influxql.DataType {
	return f.Ref.Type
}

var _ Node = &BinaryExpr{}

type BinaryExpr struct {
	LHS, RHS *ReadEdge
	Output   *WriteEdge
	Op       influxql.Token
	Desc     string
}

func (c *BinaryExpr) Description() string {
	return c.Desc
}

func (c *BinaryExpr) Inputs() []*ReadEdge   { return []*ReadEdge{c.LHS, c.RHS} }
func (c *BinaryExpr) Outputs() []*WriteEdge { return []*WriteEdge{c.Output} }

func (c *BinaryExpr) Execute() error {
	opt := influxql.IteratorOptions{}
	lhs, rhs := c.LHS.Iterator(), c.RHS.Iterator()
	itr, err := influxql.BuildTransformIterator(lhs, rhs, c.Op, opt)
	if err != nil {
		return err
	}
	c.Output.SetIterator(itr)
	return nil
}

func (c *BinaryExpr) Type() influxql.DataType {
	var lhs, rhs influxql.DataType
	if n := c.LHS.Input.Node; n != nil {
		lhs = n.Type()
	}
	if n := c.RHS.Input.Node; n != nil {
		lhs = n.Type()
	}

	if lhs.LessThan(rhs) {
		return rhs
	} else {
		return lhs
	}
}

var _ Node = &Interval{}

type Interval struct {
	TimeRange TimeRange
	Interval  influxql.Interval
	Input     *ReadEdge
	Output    *WriteEdge
}

func (i *Interval) Description() string {
	return fmt.Sprintf("normalize time values")
}

func (i *Interval) Inputs() []*ReadEdge   { return []*ReadEdge{i.Input} }
func (i *Interval) Outputs() []*WriteEdge { return []*WriteEdge{i.Output} }

func (i *Interval) Execute() error {
	opt := influxql.IteratorOptions{
		StartTime: i.TimeRange.Min.UnixNano(),
		EndTime:   i.TimeRange.Max.UnixNano(),
		Interval:  i.Interval,
	}

	input := i.Input.Iterator()
	if input == nil {
		i.Output.SetIterator(nil)
		return nil
	}
	i.Output.SetIterator(influxql.NewIntervalIterator(input, opt))
	return nil
}

func (i *Interval) Type() influxql.DataType {
	if n := i.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

var _ Node = &Limit{}

type Limit struct {
	Input  *ReadEdge
	Output *WriteEdge

	Limit  int
	Offset int
}

func (c *Limit) Description() string {
	if c.Limit > 0 && c.Offset > 0 {
		return fmt.Sprintf("limit %d/offset %d", c.Limit, c.Offset)
	} else if c.Limit > 0 {
		return fmt.Sprintf("limit %d", c.Limit)
	} else if c.Offset > 0 {
		return fmt.Sprintf("offset %d", c.Offset)
	}
	return "limit 0/offset 0"
}

func (c *Limit) Inputs() []*ReadEdge   { return []*ReadEdge{c.Input} }
func (c *Limit) Outputs() []*WriteEdge { return []*WriteEdge{c.Output} }

func (c *Limit) Execute() error {
	return errors.New("unimplemented")
}

func (c *Limit) Type() influxql.DataType {
	if n := c.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

var _ Node = &Nil{}

type Nil struct {
	Output *WriteEdge
}

func (n *Nil) Description() string {
	return "<nil>"
}

func (n *Nil) Inputs() []*ReadEdge   { return nil }
func (n *Nil) Outputs() []*WriteEdge { return []*WriteEdge{n.Output} }

func (n *Nil) Execute() error {
	n.Output.SetIterator(nil)
	return nil
}

func (n *Nil) Type() influxql.DataType {
	return influxql.Unknown
}

// Visitor visits every node in a graph.
type Visitor interface {
	// Visit is called for every node in the graph.
	// If false is returned from the function, the inputs of the
	// current node are not visited. If an error is returned,
	// processing also stops and the error is returned to Walk.
	Visit(n Node) (ok bool, err error)
}

type VisitorFunc func(n Node) (ok bool, err error)

func (fn VisitorFunc) Visit(n Node) (ok bool, err error) {
	return fn(n)
}

// Walk iterates through all of the nodes going up the tree.
// If a node is referenced as the input from multiple edges, it may be
// visited multiple times.
func Walk(n Node, v Visitor) error {
	if n == nil {
		return nil
	} else if ok, err := v.Visit(n); err != nil {
		return err
	} else if !ok {
		return nil
	}

	for _, input := range n.Inputs() {
		if input.Input.Node != nil {
			if err := Walk(input.Input.Node, v); err != nil {
				return err
			}
		}
	}
	return nil
}
