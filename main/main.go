package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"securitiesModule/securities"
	"securitiesModule/securities/securitiesSQL"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// db is the main sql database, which contains data about securuties
var db *sql.DB

// htmlDir is the directory with html files
var htmlDir string

// httpPath is the main path for http requests
var httpPath string

// generalSecurityData contains security data with last prices (string)
type generalSecurityData struct {
	ID            string
	Name          string
	Type          string
	Currency      string
	LastPriceDate string
	LastPrice     string
}

// AllSecuritiesData contains general security data for all securities (considering type and currency filters)
type AllSecuritiesData struct {
	TypeFilter     string
	CurrencyFilter string
	Securities     []generalSecurityData
}

// expSecurityQuotes contains security quotes and some extra data (string)
type expSecurityQuotes struct {
	Interval    string
	Begin       string
	End         string
	Open        string
	Close       string
	High        string
	Low         string
	Change      string
	TotalChange string
}

// securityData contains data of security (string) and expanded quotes data
type securityData struct {
	Id           string
	Name         string
	Type         string
	Currency     string
	DateFrom     string
	DateTill     string
	Interval     string
	UpdatePrices string
	ExpQuotes    []expSecurityQuotes
}

func init() {
	settingsFileName := "src\\conf.json"

	file, err := os.Open(settingsFileName)
	if err != nil {
		log.Fatal("settings file not found")
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		log.Fatal(err.Error())
	}

	type settings struct {
		HtmlDir  string
		HttpPath string
		MySQL    string
		MainDB   string
		DemoData bool
	}
	conf := settings{}
	err = json.Unmarshal(data, &conf)
	if err != nil {
		log.Fatal(err.Error())
	}

	htmlDir = conf.HtmlDir
	httpPath = conf.HttpPath
	sqlParam := conf.MySQL
	dbName := conf.MainDB
	demoData := conf.DemoData

	db, err = sql.Open("mysql", sqlParam+"/"+dbName)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		// if database doesn't exist we'll create it
		db, err = securitiesSQL.CreateDatabase(sqlParam, dbName)
		if err != nil {
			log.Fatal(err)
		}

		if demoData {
			err := securitiesSQL.PutTestDataInDatabase(db)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func main() {
	defer db.Close()

	// http requests to get json data
	http.HandleFunc("/securities/getAllSecuritiesLastQuotes", getAllSecuritiesLastQuotesHandler)
	http.HandleFunc("/securities/addSecurity", addSecurityHandler)
	http.HandleFunc("/securities/getLastQuotes", getLastQuotesHandler)
	http.HandleFunc("/securities/getSecurityData", getSecurityDataHandler)
	http.HandleFunc("/securities/delete", deleteSecurityHandler)

	// http requests to work with html pages
	http.HandleFunc("/securities", enterHandler)
	http.HandleFunc("/securities/all", allSecuritiesHandler)
	http.HandleFunc("/securities/add", addSecurityPageHandler)
	http.HandleFunc("/securities/allQuotes", getQuotesPageHandler)
	http.HandleFunc("/securities/security", securityHandler)
	http.HandleFunc("/securities/compare", compareHandler)
	http.HandleFunc("/securities/securityList", securityListHandler)

	// finish working
	err := http.ListenAndServe("localhost:8080", nil)
	log.Fatal(err)
}

// addHttpRequestParam adds new param to http GET request
func addHTTPRequestParam(request *string, paramName, paramValue string, firstParam *bool) {
	if *firstParam {
		*request += "?"
		*firstParam = false
	} else {
		*request += "&"
	}

	*request += paramName + "=" + strings.ReplaceAll(paramValue, " ", "%20")
}

// getDateFromString returns date (no time) from the given string
func getDateFromString(dateString string, defaultDate time.Time) time.Time {
	if dateString != "" {
		date, err := time.Parse("2006-01-02", dateString)
		if err != nil {
			log.Fatal(err)
		}
		return date
	}

	return defaultDate
}

// showErrorPage opens error page
func showErrorPage(writer http.ResponseWriter, errToDisplay string) {
	html, err := template.ParseFiles(htmlDir + "errorPage.html")
	if err != nil {
		log.Fatal(err) // we can't work without error page
	}

	errData := struct{ Err string }{errToDisplay}

	err = html.Execute(writer, errData)
	if err != nil {
		log.Fatal(err) // we can't work without error page
	}
}

// executeRequest executes given HTTP request
func executeRequest(writer http.ResponseWriter, request string, resStruct any) {
	resp, err := http.Get(request)
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		showErrorPage(writer, resp.Header["Err"][0])
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		showErrorPage(writer, resp.Header["Err"][0])
		return
	}

	err = json.Unmarshal(body, &resStruct)
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}
}

/////////////////////////
///// HTTP Handlers /////
/////////////////////////

// getAllSecuritiesLastQuotesHandler gets all securities of the given type and currency from database with it's last quotes
func getAllSecuritiesLastQuotesHandler(writer http.ResponseWriter, request *http.Request) {
	typeNameFilter := request.URL.Query().Get("type")
	currencyNameFilter := request.URL.Query().Get("currency")

	secList, err := securitiesSQL.GetAllSecuritiesData(db, typeNameFilter, currencyNameFilter)
	if err != nil {
		writer.Header().Set("err", err.Error())
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	wg := new(sync.WaitGroup)
	mu := new(sync.Mutex)

	generalSecData := new([]generalSecurityData)
	for _, sec := range secList {
		wg.Add(1)

		go func(sec *securities.Security) {
			defer wg.Done()

			q := sec.LastQuotes(securities.IntervalDay)

			secData := generalSecurityData{
				ID:            sec.Id(),
				Name:          sec.Name(),
				Type:          string(sec.SType()),
				Currency:      string(sec.Currency()),
				LastPriceDate: q.End.Format("02-01-2006 15:04"),
				LastPrice:     fmt.Sprintf("%f", q.Close),
			}

			mu.Lock()
			*generalSecData = append(*generalSecData, secData)
			mu.Unlock()
		}(sec)
	}

	wg.Wait()

	sort.Slice(*generalSecData, func(i, j int) bool {
		return (*generalSecData)[i].ID < (*generalSecData)[j].ID
	})

	allSecData := AllSecuritiesData{
		TypeFilter:     typeNameFilter,
		CurrencyFilter: currencyNameFilter,
		Securities:     *generalSecData,
	}

	res, err := json.Marshal(allSecData)
	if err != nil {
		writer.Header().Set("err", err.Error())
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	writer.Write(res)
}

// addSecurityHandler adds new security to database
func addSecurityHandler(writer http.ResponseWriter, request *http.Request) {
	id := request.URL.Query().Get("id")
	name := request.URL.Query().Get("name")
	typeName := request.URL.Query().Get("type")
	currencyName := request.URL.Query().Get("currency")

	if id == "" || name == "" || typeName == "" || currencyName == "" {
		writer.Header().Set("err", "not enough values")
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	sType := securities.GetSecurityTypeFromString(typeName)
	if sType == securities.UnknownType {
		writer.Header().Set("err", fmt.Sprintf("unknown type %s", typeName))
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	cur := securities.GetSecurityCurrencyFromString(currencyName)
	if cur == securities.UnknownCurrency {
		writer.Header().Set("err", fmt.Sprintf("unknown currency %s", currencyName))
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	sec := securities.GetSecurity(id, name, sType, cur)

	err := securitiesSQL.AddSecurity(db, sec)
	if err != nil {
		writer.Header().Set("err", err.Error())
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	writer.WriteHeader(http.StatusOK)
}

// getLastQuotesHandler gets last quotes for all securities
func getLastQuotesHandler(writer http.ResponseWriter, request *http.Request) {
	securitiesSQL.UpdateAllSecuritiesLastQuotes(db, "", "")
}

// getSecurityDataHandler gets security data and quotes
func getSecurityDataHandler(writer http.ResponseWriter, request *http.Request) {
	var err error

	id := request.URL.Query().Get("id")
	typeString := request.URL.Query().Get("type")
	dateFromString := request.URL.Query().Get("dateFrom")
	dateTillString := request.URL.Query().Get("dateTill")
	intervalString := request.URL.Query().Get("interval")
	updatePricesString := request.URL.Query().Get("updatePrices")

	if id == "" || typeString == "" {
		writer.Header().Set("err", "not enough values")
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	sType := securities.GetSecurityTypeFromString(typeString)
	if sType == securities.UnknownType {
		writer.Header().Set("err", fmt.Sprintf("unknown type %s", typeString))
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	qInterval := securities.IntervalDay
	if intervalString != "" {
		qInterval, err = strconv.Atoi(intervalString)
		if err != nil {
			writer.Header().Set("err", err.Error())
			writer.WriteHeader(http.StatusNoContent)
			return
		}
	}

	dateFrom := getDateFromString(dateFromString, time.Now().Truncate(time.Hour*24).AddDate(0, -1, 0)).UTC()
	dateTill := getDateFromString(dateTillString, time.Now().Truncate(time.Hour*24)).Add(time.Second * (60*60*24 - 1)).UTC()
	if dateFrom.After(dateTill) {
		writer.Header().Set("err", "date from can't be after date till")
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	updatePrices := updatePricesString == "true"

	if updatePrices {
		sec := securities.GetQuickSecurity(id, sType)

		err = securitiesSQL.UpdateSecurityQuotes(db, sec, dateFrom, dateTill, securities.QuotesInterval(qInterval))
		if err != nil {
			writer.Header().Set("err", err.Error())
			writer.WriteHeader(http.StatusNoContent)
			return
		}
	}

	sec := securities.GetQuickSecurity(id, sType)

	err = securitiesSQL.GetSecurityData(db, sec)
	if err != nil {
		writer.Header().Set("err", err.Error())
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	quotes := *sec.QuotesOfInterval(securities.QuotesInterval(qInterval))
	expSeqQuotes := new([]expSecurityQuotes)

	startPrice := 0.0
	prevPrice := 0.0
	for _, q := range quotes {
		if dateFrom.After(q.End) || q.End.After(dateTill) {
			continue
		}

		totalChange := 0.0
		if startPrice != 0.0 {
			totalChange = (q.Close - startPrice) / startPrice * 100
		} else {
			startPrice = q.Close
		}

		change := 0.0
		if prevPrice != 0.0 {
			change = (q.Close - prevPrice) / prevPrice * 100
		}
		prevPrice = q.Close

		sQuotes := expSecurityQuotes{
			Interval:    fmt.Sprint(qInterval),
			Begin:       q.Begin.Format("02.01.2006 15:04:05"),
			End:         q.End.Format("02.01.2006 15:04:05"),
			Open:        fmt.Sprintf("%f", q.Open),
			Close:       fmt.Sprintf("%f", q.Close),
			High:        fmt.Sprintf("%f", q.High),
			Low:         fmt.Sprintf("%f", q.Low),
			Change:      fmt.Sprintf("%.2f", change),
			TotalChange: fmt.Sprintf("%.2f", totalChange),
		}

		*expSeqQuotes = append(*expSeqQuotes, sQuotes)
	}

	secData := securityData{
		Id:           sec.Id(),
		Name:         sec.Name(),
		Type:         string(sec.SType()),
		Currency:     string(sec.Currency()),
		DateFrom:     dateFrom.Format("2006-01-02"),
		DateTill:     dateTill.Format("2006-01-02"),
		Interval:     fmt.Sprint(qInterval),
		UpdatePrices: updatePricesString,
		ExpQuotes:    *expSeqQuotes,
	}

	res, err := json.Marshal(secData)
	if err != nil {
		writer.Header().Set("err", err.Error())
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	writer.Write(res)
}

// deleteSecurityHandler deletes security from database
func deleteSecurityHandler(writer http.ResponseWriter, request *http.Request) {
	id := request.URL.Query().Get("id")
	typeString := request.URL.Query().Get("type")

	if id == "" || typeString == "" {
		writer.Header().Set("err", "not enough values")
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	sType := securities.GetSecurityTypeFromString(typeString)
	if sType == securities.UnknownType {
		writer.Header().Set("err", fmt.Sprintf("unknown type %s", typeString))
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	sec := securities.GetQuickSecurity(id, sType)

	err := securitiesSQL.DeleteSecurity(db, sec)
	if err != nil {
		writer.Header().Set("err", err.Error())
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	http.Redirect(writer, request, "/securities", http.StatusPermanentRedirect)
}

/////////////////////////
///// HTML Handlers /////
/////////////////////////

// enterHandler opens main page to choose next activity
func enterHandler(writer http.ResponseWriter, request *http.Request) {
	html, err := template.ParseFiles(htmlDir + "mainPage.html")
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}

	err = html.Execute(writer, nil)
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}
}

// allSecuritiesHandler opens the list of all existing in database securities
func allSecuritiesHandler(writer http.ResponseWriter, request *http.Request) {
	html, err := template.ParseFiles(htmlDir + "allSecurities.html")
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}

	typeNameFilter := request.FormValue("typeFilter")
	currencyNameFilter := request.FormValue("currencyFilter")

	req := httpPath + "/securities/getAllSecuritiesLastQuotes"
	firstParam := true
	if typeNameFilter != "" {
		addHTTPRequestParam(&req, "type", typeNameFilter, &firstParam)
	}
	if currencyNameFilter != "" {
		addHTTPRequestParam(&req, "currency", currencyNameFilter, &firstParam)
	}

	resStruct := &AllSecuritiesData{}
	executeRequest(writer, req, resStruct)

	err = html.Execute(writer, *resStruct)
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}
}

// addSecurityPageHandler opens the page to add new security to database
func addSecurityPageHandler(writer http.ResponseWriter, request *http.Request) {
	html, err := template.ParseFiles(htmlDir + "addSecurity.html")
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}

	id := request.FormValue("id")
	name := request.FormValue("name")
	typeName := request.FormValue("type")
	currencyName := request.FormValue("currency")

	if id == "" || name == "" || typeName == "" || currencyName == "" {
		err = html.Execute(writer, struct {
			Id       string
			Name     string
			Type     string
			Currency string
		}{Id: id,
			Name:     name,
			Type:     typeName,
			Currency: currencyName})
		if err != nil {
			showErrorPage(writer, err.Error())
		}

		return
	}

	req := httpPath + "/securities/addSecurity"
	firstParam := true
	addHTTPRequestParam(&req, "id", id, &firstParam)
	addHTTPRequestParam(&req, "name", name, &firstParam)
	addHTTPRequestParam(&req, "type", typeName, &firstParam)
	addHTTPRequestParam(&req, "currency", currencyName, &firstParam)

	resp, err := http.Get(req)
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		showErrorPage(writer, resp.Header[http.CanonicalHeaderKey("err")][0])
		return
	}

	http.Redirect(writer, request, "/securities/all", http.StatusPermanentRedirect)
}

// getQuotesPageHandler gets last quotes for all securities
func getQuotesPageHandler(writer http.ResponseWriter, request *http.Request) {
	req := httpPath + "/securities/getLastQuotes"

	_, err := http.Get(req)
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}

	http.Redirect(writer, request, "/securities/all", http.StatusPermanentRedirect)
}

// securityHandler shows prices of the given security for the given period
func securityHandler(writer http.ResponseWriter, request *http.Request) {
	html, err := template.ParseFiles(htmlDir + "securityData.html")
	if err != nil {
		log.Fatal(err)
	}

	id := request.FormValue("id")
	typeString := request.FormValue("type")
	dateFromString := request.FormValue("dateFrom")
	dateTillString := request.FormValue("dateTill")
	updatePrices := request.FormValue("updatePrices")

	if id == "" || typeString == "" {
		err := html.Execute(writer, struct {
			Id           string
			Name         string
			Type         string
			DateFrom     string
			DateTill     string
			UpdatePrices string
			ExpQuotes    []expSecurityQuotes
		}{Id: id,
			Name:         "",
			Type:         typeString,
			DateFrom:     dateFromString,
			DateTill:     dateTillString,
			UpdatePrices: updatePrices,
			ExpQuotes:    *new([]expSecurityQuotes)})

		if err != nil {
			showErrorPage(writer, err.Error())
			return
		}
		return
	}

	req := httpPath + "/securities/getSecurityData"
	firstParam := true
	addHTTPRequestParam(&req, "id", id, &firstParam)
	addHTTPRequestParam(&req, "type", typeString, &firstParam)
	if dateFromString != "" {
		addHTTPRequestParam(&req, "dateFrom", dateFromString, &firstParam)
	}
	if dateTillString != "" {
		addHTTPRequestParam(&req, "dateTill", dateTillString, &firstParam)
	}
	if updatePrices != "" {
		addHTTPRequestParam(&req, "updatePrices", "true", &firstParam)
	}

	resStruct := &securityData{}
	executeRequest(writer, req, resStruct)

	err = html.Execute(writer, *resStruct)
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}
}

// compareHandler shows comparison of two given securities for the given period
func compareHandler(writer http.ResponseWriter, request *http.Request) {
	html, err := template.ParseFiles(htmlDir + "compareSecurities.html")
	if err != nil {
		log.Fatal(err)
	}

	type compQuotes struct {
		Date        string
		Price1      string
		Price2      string
		DayProfit   string
		TotalProfit string
	}

	id1 := request.FormValue("id1")
	id2 := request.FormValue("id2")
	typeString := request.FormValue("type")
	dateFromString := request.FormValue("dateFrom")
	dateTillString := request.FormValue("dateTill")

	htmlData := struct {
		Id1       string
		Id2       string
		Type      string
		DateFrom  string
		DateTill  string
		ExpQuotes map[time.Time]*compQuotes
	}{}

	if id1 == "" || id2 == "" || typeString == "" {
		htmlData.Id1 = id1
		htmlData.Id2 = id2
		htmlData.Type = typeString
		htmlData.DateFrom = dateFromString
		htmlData.DateTill = dateTillString
		htmlData.ExpQuotes = *new(map[time.Time]*compQuotes)

		err := html.Execute(writer, htmlData)
		if err != nil {
			showErrorPage(writer, err.Error())
			return
		}
		return
	}

	// it would be probably better to make new request here

	reqResult := func(id string) *securityData {
		req := httpPath + "/securities/getSecurityData"
		firstParam := true
		addHTTPRequestParam(&req, "id", id, &firstParam)
		addHTTPRequestParam(&req, "type", typeString, &firstParam)
		if dateFromString != "" {
			addHTTPRequestParam(&req, "dateFrom", dateFromString, &firstParam)
		}
		if dateTillString != "" {
			addHTTPRequestParam(&req, "dateTill", dateTillString, &firstParam)
		}

		resStruct := &securityData{}
		executeRequest(writer, req, resStruct)

		return resStruct
	}

	quotes1 := *reqResult(id1)
	quotes2 := *reqResult(id2)

	result := make(map[time.Time]*compQuotes)

	for _, q := range quotes1.ExpQuotes {
		date, err := time.Parse("02.01.2006 15:04:05", q.End)
		if err != nil {
			showErrorPage(writer, err.Error())
			return
		}

		date = date.Truncate(time.Hour * 24)
		if _, ok := result[date]; !ok {
			result[date] = new(compQuotes)
		}

		result[date].Date = date.Format("02.01.2006")
		result[date].Price1 = q.Close
	}

	for _, q := range quotes2.ExpQuotes {
		date, err := time.Parse("02.01.2006 15:04:05", q.End)
		if err != nil {
			showErrorPage(writer, err.Error())
			return
		}

		date = date.Truncate(time.Hour * 24)
		if _, ok := result[date]; !ok {
			result[date] = new(compQuotes)
		}

		result[date].Date = date.Format("02.01.2006")
		result[date].Price2 = q.Close
	}

	dateFrom := getDateFromString(dateFromString, time.Now().Truncate(time.Hour*24).AddDate(0, -1, 0)).UTC()
	dateTill := getDateFromString(dateTillString, time.Now().Truncate(time.Hour*24)).Add(time.Second * (60*60*24 - 1)).UTC()
	startPrice1 := 0.0
	prevPrice1 := 0.0
	startPrice2 := 0.0
	prevPrice2 := 0.0
	for date := dateFrom; !date.After(dateTill); date = date.AddDate(0, 0, 1) {
		q, ok := result[date]
		if !ok {
			continue
		}

		if q.Price1 == "" || q.Price2 == "" {
			continue
		}

		pr1, err := strconv.ParseFloat(q.Price1, 64)
		if err != nil {
			showErrorPage(writer, err.Error())
			return
		}

		dayChange1 := 0.0
		if prevPrice1 > 0.0 {
			dayChange1 = (pr1 - prevPrice1) / prevPrice1 * 100
		}
		prevPrice1 = pr1

		totalChange1 := 0.0
		if startPrice1 > 0.0 {
			totalChange1 = (pr1 - startPrice1) / startPrice1 * 100
		} else {
			startPrice1 = pr1
		}

		pr2, err := strconv.ParseFloat(q.Price2, 64)
		if err != nil {
			showErrorPage(writer, err.Error())
			return
		}

		dayChange2 := 0.0
		if prevPrice2 > 0.0 {
			dayChange2 = (pr2 - prevPrice2) / prevPrice2 * 100
		}
		prevPrice2 = pr2

		totalChange2 := 0.0
		if startPrice2 > 0.0 {
			totalChange2 = (pr2 - startPrice2) / startPrice2 * 100
		} else {
			startPrice2 = pr2
		}

		result[date].DayProfit = fmt.Sprintf("%.2f", dayChange1-dayChange2)
		result[date].TotalProfit = fmt.Sprintf("%.2f", totalChange1-totalChange2)
	}

	htmlData.Id1 = id1
	htmlData.Id2 = id2
	htmlData.Type = typeString
	htmlData.DateFrom = dateFromString
	htmlData.DateTill = dateTillString
	htmlData.ExpQuotes = result

	err = html.Execute(writer, htmlData)
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}
}

// securityListHandler adds to database the list of securities from the given file with quotes for the given period
// Then the list of securities with begin and end quotes is written down to another file sorted by change %
func securityListHandler(writer http.ResponseWriter, request *http.Request) {
	// TODO: add currency and security names
	// TODO: add some more checks about file content

	html, err := template.ParseFiles(htmlDir + "securityList.html")
	if err != nil {
		log.Fatal(err)
	}

	type secPrices struct {
		id         string
		priceBegin float64
		priceEnd   float64
		change     float64
	}

	var secSlice []*securities.Security
	var secQuotes []secPrices

	typeString := request.FormValue("type")
	dateFromString := request.FormValue("dateFrom")
	dateTillString := request.FormValue("dateTill")
	fileName := request.FormValue("fileName")

	if typeString == "" || fileName == "" {
		err := html.Execute(writer, struct {
			Type     string
			DateFrom string
			DateTill string
			FileName string
		}{Type: typeString,
			DateFrom: dateFromString,
			DateTill: dateTillString,
			FileName: fileName})

		if err != nil {
			showErrorPage(writer, err.Error())
			return
		}
		return
	}

	sType := securities.GetSecurityTypeFromString(typeString)
	if sType == securities.UnknownType {
		showErrorPage(writer, fmt.Sprintf("unknown type %s", typeString))
		return
	}

	file, err := os.Open(fileName)
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}
	if os.IsNotExist(err) {
		showErrorPage(writer, "file not found")
		return
	}
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		secSlice = append(secSlice, securities.GetQuickSecurity(scanner.Text(), sType))
	}
	if scanner.Err() != nil {
		showErrorPage(writer, err.Error())
		return
	}

	dateFrom := getDateFromString(dateFromString, time.Now().Truncate(time.Hour*24).AddDate(0, -1, 0)).UTC()
	dateTill := getDateFromString(dateTillString, time.Now().Truncate(time.Hour*24)).Add(time.Second * (60*60*24 - 1)).UTC()
	if dateFrom.After(dateTill) {
		showErrorPage(writer, "date from can't be after date till")
		return
	}

	err = securitiesSQL.AddSecurities(db, secSlice)
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}

	wg := new(sync.WaitGroup)
	mu := new(sync.Mutex)

	for _, sec := range secSlice {
		wg.Add(1)

		go func(sec *securities.Security) {
			defer wg.Done()

			err = securitiesSQL.UpdateSecurityQuotes(db, sec, dateFrom, dateTill, securities.IntervalDay)
			if err != nil {
				return // we will just ignore wrong securities for now
			}

			priceBegin := sec.QuotesForDate(securities.IntervalDay, dateFrom.Truncate(time.Hour*24).AddDate(0, 0, 1)).Open
			priceEnd := sec.QuotesForDate(securities.IntervalDay, dateTill.Truncate(time.Hour*24).AddDate(0, 0, 1)).Close
			change := 0.0
			if priceBegin > 0.0 {
				change = math.Round((priceEnd-priceBegin)/priceBegin*10000) / 100
			}

			secPr := secPrices{
				id:         sec.Id(),
				priceBegin: priceBegin,
				priceEnd:   priceEnd,
				change:     change,
			}

			mu.Lock()
			secQuotes = append(secQuotes, secPr)
			mu.Unlock()
		}(sec)
	}

	wg.Wait()

	sort.Slice(secQuotes, func(i, j int) bool {
		return secQuotes[i].change < secQuotes[j].change || (secQuotes[i].change == secQuotes[j].change && secQuotes[i].id < secQuotes[j].id)
	})

	options := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	fileRes, err := os.OpenFile(strings.Split(fileName, ".")[0]+"_result.txt", options, os.FileMode(0600))
	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}
	defer fileRes.Close()

	for _, secListPrice := range secQuotes {
		_, err = fmt.Fprintf(fileRes, "%s\t - %f\t - %f\t - %.2f\n", secListPrice.id, secListPrice.priceBegin, secListPrice.priceEnd, secListPrice.change)
		if err != nil {
			showErrorPage(writer, err.Error())
			return
		}
	}

	err = html.Execute(writer, struct {
		Type     string
		DateFrom string
		DateTill string
		FileName string
	}{Type: typeString,
		DateFrom: dateFromString,
		DateTill: dateTillString,
		FileName: fileName})

	if err != nil {
		showErrorPage(writer, err.Error())
		return
	}
}
