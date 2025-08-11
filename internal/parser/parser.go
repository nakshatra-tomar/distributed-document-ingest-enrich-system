package parser

import (
    "fmt"
    "sort"
    "strings"
)

// Parser defines the interface for document parsers.
// Implementations should declare the file types they handle (e.g. "txt", "pdf", "eml")
// and return a normalized text representation from raw bytes.
type Parser interface {
    Parse(content []byte) (string, error)
    SupportedTypes() []string
}

// Registry stores parsers by (lowercased) file type and resolves them on demand.
type Registry struct {
    parsers map[string]Parser
}

// NewRegistry creates a new parser registry and registers default parsers.
func NewRegistry() *Registry {
    r := &Registry{
        parsers: make(map[string]Parser),
    }
    // Register built-in parsers. These constructors must exist in your package.
    r.Register(NewTextParser())
    r.Register(NewPDFParser())
    r.Register(NewEmailParser())
    return r
}

// Register adds a parser for all of its supported types.
// Later registrations for the same type will overwrite earlier ones.
func (r *Registry) Register(p Parser) {
    for _, t := range p.SupportedTypes() {
        r.parsers[strings.ToLower(t)] = p
    }
}

// GetParser returns a parser for the given file type (case-insensitive).
func (r *Registry) GetParser(fileType string) (Parser, error) {
    ft := strings.ToLower(strings.TrimPrefix(fileType, "."))
    if p, ok := r.parsers[ft]; ok {
        return p, nil
    }
    return nil, fmt.Errorf("no parser found for file type: %s", fileType)
}

// SupportedTypes returns a sorted list of all registered file types.
func (r *Registry) SupportedTypes() []string {
    types := make([]string, 0, len(r.parsers))
    for t := range r.parsers {
        types = append(types, t)
    }
    sort.Strings(types)
    return types
}

// ParseByType is a convenience helper that resolves a parser by file type and parses content.
func (r *Registry) ParseByType(fileType string, content []byte) (string, error) {
    p, err := r.GetParser(fileType)
    if err != nil {
        return "", err
    }
    return p.Parse(content)
}
