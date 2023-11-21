// Package securities defines the type Security and some methods to work with it
package securities

import (
	"strings"
	"time"
)

// SecurityType is a type of security - share, bond etc
type SecurityType string

const (
	UnknownType SecurityType = "unknown"
	Share       SecurityType = "share"
	ETF         SecurityType = "etf"
	Bond        SecurityType = "bond"
	Currency    SecurityType = "currency"
)

// SecurityCurrency is a currency of security - RUB, USD etc
// There are only limited number of currencies on Moscow exchange, so it's reasonable to list them all
type SecurityCurrency string

const (
	UnknownCurrency SecurityCurrency = "unknown"
	RUB             SecurityCurrency = "RUB"
	USD             SecurityCurrency = "USD"
	EUR             SecurityCurrency = "EUR"
	CNY             SecurityCurrency = "CNY"
)

// QuotesInterval is a type of quotes interval - day, hour etc
type QuotesInterval int

const (
	IntervalUnknown = 0
	IntervalQuarter = 4
	IntervalMonth   = 31
	IntervalWeek    = 7
	IntervalDay     = 24
	IntervalHour    = 60
	IntervalTenMin  = 10
	IntervalMinute  = 1
)

// SecurityQuotes is a struct with information about security quotes
type SecurityQuotes struct {
	Interval QuotesInterval
	Begin    time.Time
	End      time.Time
	Open     float64
	Close    float64
	High     float64
	Low      float64
}

// Security is a struct with information about security
type Security struct {
	id       string
	name     string
	sType    SecurityType
	currency SecurityCurrency
	quotes   *[]SecurityQuotes
}

// GetSecurity creates a new security with no quotes
func GetSecurity(id string, name string, sType SecurityType, currency SecurityCurrency) *Security {
	return &Security{
		id:       strings.ToUpper(id),
		name:     name,
		sType:    sType,
		currency: currency,
		quotes:   new([]SecurityQuotes),
	}
}

// GetQuickSecurity creates a new security with only id and type
func GetQuickSecurity(id string, sType SecurityType) *Security {
	return GetSecurity(id, "", sType, UnknownCurrency)
}

// SetName sets the name of security
func (s *Security) SetName(name string) {
	s.name = name
}

// SetCurrency sets the currency of security
func (s *Security) SetCurrency(currency SecurityCurrency) {
	s.currency = currency
}

// SetQuotes sets the quotes of security (without clearing existing quotes)
func (s *Security) SetQuotes(quotes SecurityQuotes) {
	*s.quotes = append(*s.quotes, quotes)
}

// SetQuotesList sets the list of security quotes (without clearing existing quotes)
func (s *Security) SetQuotesList(quotes *[]SecurityQuotes) {
	*s.quotes = append(*s.quotes, *quotes...)
}

// ClearAndSetQuotesList clears and sets the list of security quotes
func (s *Security) ClearAndSetQuotesList(quotes *[]SecurityQuotes) {
	s.quotes = new([]SecurityQuotes)
	s.SetQuotesList(quotes)
}

// Id returns the id of security
func (s *Security) Id() string {
	return s.id
}

// Name returns the name of security
func (s *Security) Name() string {
	return s.name
}

// SecType returns the type of security
func (s *Security) SType() SecurityType {
	return s.sType
}

// Currency returns the currency of security
func (s *Security) Currency() SecurityCurrency {
	return s.currency
}

// Quotes returns all security quotes
func (s *Security) Quotes() *[]SecurityQuotes {
	return s.quotes
}

// QuotesOfInterval returns all security quotes of the given interval
func (s *Security) QuotesOfInterval(interval QuotesInterval) *[]SecurityQuotes {
	quotes := new([]SecurityQuotes)

	for _, q := range *s.quotes {
		if q.Interval == interval {
			*quotes = append(*quotes, q)
		}
	}

	return quotes
}

// QuotesForDate returns the last quotes of the given interval of security for the given date
func (s *Security) QuotesForDate(interval QuotesInterval, date time.Time) SecurityQuotes {
	var quotes SecurityQuotes

	for _, q := range *s.quotes {
		if q.Interval != interval || q.End.After(date) || quotes.End.After(q.End) {
			continue
		}

		quotes = q
	}

	return quotes
}

// LastQuotes returns the last quotes of the given interval of security
func (s *Security) LastQuotes(interval QuotesInterval) SecurityQuotes {
	return s.QuotesForDate(interval, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
}

// GetSecurityTypeFromString converts string type of security to SecurityType
func GetSecurityTypeFromString(typeName string) SecurityType {
	switch strings.ToLower(typeName) {
	case "share":
		return Share
	case "etf":
		return ETF
	case "bond":
		return Bond
	case "currency":
		return Currency
	default:
		return UnknownType
	}
}

// GetSecurityCurrencyFromString converts string currency to SecurityCurrency
func GetSecurityCurrencyFromString(currencyName string) SecurityCurrency {
	switch strings.ToUpper(currencyName) {
	case "RUB":
		return RUB
	case "USD":
		return USD
	case "EUR":
		return EUR
	case "CNY":
		return CNY
	default:
		return UnknownCurrency
	}
}
