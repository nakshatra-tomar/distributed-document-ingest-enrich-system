package enrich

import (
	"math"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"distributed-document-ingest-enrich-system/internal/models"
)

// Precompiled regexes (avoid recompiling every call).
var (
	reNonLetters    = regexp.MustCompile(`[^\p{L}]+`)                     // split on non‑letters
	reSentenceSplit = regexp.MustCompile(`[.!?]+\s+`)                     // naive sentence split
	reEmail         = regexp.MustCompile(`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b`)
	rePhone         = regexp.MustCompile(`\b\d{3}[-.]?\d{3}[-.]?\d{4}\b`)
	reDate          = regexp.MustCompile(`\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b|\b\d{4}-\d{2}-\d{2}\b`)
	reMoney         = regexp.MustCompile(`\$\d+(?:,\d{3})*(?:\.\d{2})?`)
)

// NLPProcessor handles lightweight NLP tasks (keywords, language, summary, sentiment, entities).
type NLPProcessor struct {
	stopWords      map[string]bool
	commonWords    map[string]int
	patterns       map[models.DocumentType]*regexp.Regexp
	sentimentWords map[string]int
}

// NewNLPProcessor creates a new processor with built‑in resources.
func NewNLPProcessor() *NLPProcessor {
	return &NLPProcessor{
		stopWords:      buildStopWords(),
		commonWords:    buildCommonWords(),
		patterns:       buildPatterns(),
		sentimentWords: buildSentimentWords(),
	}
}

// ExtractKeywords returns up to maxKeywords tokens scored by a TF/IDF‑like heuristic.
func (nlp *NLPProcessor) ExtractKeywords(text string, maxKeywords int) []string {
	if maxKeywords <= 0 {
		maxKeywords = 10
	}

	words := nlp.tokenize(text)
	if len(words) == 0 {
		return nil
	}

	// term frequencies
	wordFreq := make(map[string]int)
	total := 0
	for _, w := range words {
		if nlp.isValidKeyword(w) {
			wordFreq[w]++
			total++
		}
	}
	if total == 0 {
		return nil
	}

	type wordScore struct {
		word  string
		score float64
	}

	// TF * (cheap IDF)
	scores := make([]wordScore, 0, len(wordFreq))
	for w, f := range wordFreq {
		tf := float64(f) / float64(total)

		commonFreq, ok := nlp.commonWords[w]
		idf := 1.0
		if ok {
			idf = math.Log(10000.0 / float64(commonFreq+1))
		} else {
			idf = math.Log(10000.0)
		}

		s := tf * idf

		// light boosts
		if len(w) > 6 {
			s *= 1.2
		}
		if len(w) > 0 && unicode.IsUpper(rune(w[0])) {
			s *= 1.1
		}

		scores = append(scores, wordScore{w, s})
	}

	sort.Slice(scores, func(i, j int) bool { return scores[i].score > scores[j].score })

	n := maxKeywords
	if len(scores) < n {
		n = len(scores)
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, scores[i].word)
	}
	return out
}

// DetectLanguage does a tiny bag‑of‑words heuristic for English.
func (nlp *NLPProcessor) DetectLanguage(text string) string {
	if len(text) < 50 {
		return "unknown"
	}

	t := " " + strings.ToLower(text) + " "
	englishWords := []string{
		"the", "and", "is", "in", "to", "of", "a", "that", "it", "with",
		"for", "as", "was", "on", "are", "you", "this", "be", "at", "have",
	}

	matches := 0
	for _, w := range englishWords {
		if strings.Contains(t, " "+w+" ") {
			matches++
		}
	}
	if float64(matches)/float64(len(englishWords)) > 0.3 {
		return "en"
	}
	return "unknown"
}

// ClassifyDocument returns a coarse type from filename hints + content regexes.
func (nlp *NLPProcessor) ClassifyDocument(filename, content string) models.DocumentType {
	filename = strings.ToLower(filename)
	content = strings.ToLower(content)

	scores := make(map[models.DocumentType]int)

	// filename hints
	filenameHints := map[string]models.DocumentType{
		"report": models.DocTypeReport,
		"invoice": models.DocTypeInvoice, "bill": models.DocTypeInvoice,
		"log": models.DocTypeLog,
		"email": models.DocTypeEmail, "eml": models.DocTypeEmail,
		"legal": models.DocTypeLegal, "contract": models.DocTypeLegal,
	}
	for hint, dt := range filenameHints {
		if strings.Contains(filename, hint) {
			scores[dt] += 3
		}
	}

	// content patterns
	for dt, rx := range nlp.patterns {
		scores[dt] += len(rx.FindAllString(content, -1))
	}

	best, max := models.DocTypeUnknown, 0
	for dt, sc := range scores {
		if sc > max {
			max, best = sc, dt
		}
	}
	return best
}

// GenerateSummary picks top sentences by keyword density + position bias.
func (nlp *NLPProcessor) GenerateSummary(text string, maxSentences int) string {
	if maxSentences <= 0 {
		maxSentences = 3
	}

	sentences := nlp.splitSentences(text)
	if len(sentences) <= maxSentences {
		return text
	}

	keywords := nlp.ExtractKeywords(text, 20)
	kw := make(map[string]bool, len(keywords))
	for _, k := range keywords {
		kw[k] = true
	}

	type sscore struct {
		s        string
		score    float64
		position int
	}

	scores := make([]sscore, 0, len(sentences))
	for i, s := range sentences {
		toks := nlp.tokenize(s)
		if len(toks) == 0 {
			continue
		}

		// keyword density
		kc := 0
		for _, w := range toks {
			if kw[w] {
				kc++
			}
		}
		score := float64(kc) / float64(len(toks)) * 10

		// position boosts
		if i < len(sentences)/4 {
			score += 2.0
		}
		if i > 3*len(sentences)/4 {
			score += 1.0
		}

		// length penalties
		if len(toks) < 5 {
			score *= 0.5
		}
		if len(toks) > 30 {
			score *= 0.8
		}

		scores = append(scores, sscore{s: s, score: score, position: i})
	}

	sort.Slice(scores, func(i, j int) bool {
		if math.Abs(scores[i].score-scores[j].score) < 0.1 {
			return scores[i].position < scores[j].position
		}
		return scores[i].score > scores[j].score
	})

	n := maxSentences
	if len(scores) < n {
		n = len(scores)
	}
	top := append([]sscore(nil), scores[:n]...)
	sort.Slice(top, func(i, j int) bool { return top[i].position < top[j].position })

	var b strings.Builder
	for i, s := range top {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(strings.TrimSpace(s.s))
	}
	return b.String()
}

// AnalyzeSentiment does lexicon‑based polarity scoring.
func (nlp *NLPProcessor) AnalyzeSentiment(text string) string {
	words := nlp.tokenize(text)
	if len(words) == 0 {
		return "neutral"
	}

	pos, neg := 0, 0
	for _, w := range words {
		if sc, ok := nlp.sentimentWords[w]; ok {
			if sc > 0 {
				pos += sc
			} else {
				neg += -sc
			}
		}
	}

	switch {
	case float64(pos) > float64(neg)*1.2:
		return "positive"
	case float64(neg) > float64(pos)*1.2:
		return "negative"
	default:
		return "neutral"
	}
}

// ExtractEntities finds a few common patterns (email/phone/date/money).
func (nlp *NLPProcessor) ExtractEntities(text string) map[string][]string {
	entities := make(map[string][]string)
	if m := reEmail.FindAllString(text, -1); len(m) > 0 {
		entities["emails"] = m
	}
	if m := rePhone.FindAllString(text, -1); len(m) > 0 {
		entities["phones"] = m
	}
	if m := reDate.FindAllString(text, -1); len(m) > 0 {
		entities["dates"] = m
	}
	if m := reMoney.FindAllString(text, -1); len(m) > 0 {
		entities["money"] = m
	}
	return entities
}

// --- helpers ---

func (nlp *NLPProcessor) tokenize(text string) []string {
	parts := reNonLetters.Split(strings.ToLower(text), -1)
	out := make([]string, 0, len(parts))
	for _, w := range parts {
		w = strings.TrimSpace(w)
		if len(w) > 2 {
			out = append(out, w)
		}
	}
	return out
}

func (nlp *NLPProcessor) isValidKeyword(w string) bool {
	if len(w) < 3 || nlp.stopWords[w] {
		return false
	}
	for _, r := range w {
		if !unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

func (nlp *NLPProcessor) splitSentences(text string) []string {
	parts := reSentenceSplit.Split(text, -1)
	out := make([]string, 0, len(parts))
	for _, s := range parts {
		s = strings.TrimSpace(s)
		if len(s) > 10 {
			out = append(out, s)
		}
	}
	return out
}

// --- data builders ---

func buildStopWords() map[string]bool {
	words := []string{
		"a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
		"has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
		"to", "was", "will", "with", "would", "you", "your", "have", "had",
		"been", "were", "said", "each", "which", "she", "do", "how", "their",
		"if", "up", "out", "many", "then", "them", "these", "so", "some", "her",
		"would", "make", "like", "into", "him", "time", "two", "more", "go", "no",
		"way", "could", "my", "than", "first", "water", "long", "little", "very",
		"after", "words", "without", "just", "where", "too", "any", "new", "work",
		"part", "take", "get", "place", "made", "live", "back", "only", "years",
		"came", "show", "every", "good", "me", "give", "our", "under", "name",
	}
	m := make(map[string]bool, len(words))
	for _, w := range words {
		m[w] = true
	}
	return m
}

func buildCommonWords() map[string]int {
	return map[string]int{
		"system": 1000, "process": 800, "data": 900,
		"information": 700, "service": 600, "user": 850,
		"application": 500, "business": 400, "management": 350,
		"development": 300, "technology": 250, "solution": 200,
		"project": 180, "analysis": 160, "report": 140,
		"document": 120, "content": 100, "company": 90,
		"customer": 80, "market": 70, "product": 60,
	}
}

func buildPatterns() map[models.DocumentType]*regexp.Regexp {
	return map[models.DocumentType]*regexp.Regexp{
		models.DocTypeEmail:   regexp.MustCompile(`(?i)(from:|to:|subject:|dear\s+\w+|regards|sincerely|best\s+regards)`),
		models.DocTypeLog:     regexp.MustCompile(`(?i)(\d{4}-\d{2}-\d{2}|\[?(ERROR|WARN|INFO|DEBUG)\]?|exception|stack\s+trace|failed|error)`),
		models.DocTypeInvoice: regexp.MustCompile(`(?i)(invoice\s+#?\d+|total\s+amount|due\s+date|payment\s+terms|billing|amount\s+due)`),
		models.DocTypeReport:  regexp.MustCompile(`(?i)(executive\s+summary|quarterly|annual|analysis|conclusion|recommendations|performance|metrics)`),
		models.DocTypeLegal:   regexp.MustCompile(`(?i)(whereas|hereby|agreement|contract|terms\s+and\s+conditions|liability|clause|party)`),
	}
}

func buildSentimentWords() map[string]int {
	return map[string]int{
		// Positive
		"good": 2, "great": 3, "excellent": 3, "amazing": 3, "wonderful": 3,
		"fantastic": 3, "outstanding": 3, "perfect": 3, "success": 2, "successful": 2,
		"positive": 2, "happy": 2, "pleased": 2, "satisfied": 2, "love": 3,
		"like": 1, "enjoy": 2, "appreciate": 2, "thank": 2, "thanks": 2,
		// Negative
		"bad": -2, "terrible": -3, "awful": -3, "horrible": -3, "worst": -3,
		"hate": -3, "dislike": -2, "disappointed": -2, "frustrated": -2, "angry": -2,
		"sad": -2, "upset": -2, "problem": -1, "issue": -1, "error": -2,
		"fail": -2, "failed": -2, "failure": -2, "wrong": -1, "difficult": -1,
	}
}
