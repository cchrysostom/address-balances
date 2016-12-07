package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/mattn/go-sqlite3"
)

type Payment struct {
	DestAddress    string
	PaymentAddress string
	TargetBalance  int
	PayableBalance int
	CurrentBalance int
}

type BitcoinAddress struct {
	Address  string `json:"address"`
	Balance  int    `json:"final_balance"`
	Received int    `json:"total_received"`
	Sent     int    `json:"total_sent"`
}

func main() {
	db, err := sql.Open("sqlite3", "/home/chris/Documents/Alexandria/payproc/payproc.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var pymtToDest map[string]Payment
	pymtToDest = make(map[string]Payment)

	sqlStmt := `
      select paymentAddress, destinationAddress, targetBalance, payableBalance
        from PaymentAddress
        where destinationAddress != ''
        order by destinationAddress, paymentAddress
	`
	rows, err := db.Query(sqlStmt)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var pymtAddr string
		var destAddr string
		var targetBal int
		var payableBal int
		rows.Scan(&pymtAddr, &destAddr, &targetBal, &payableBal)
		pymtFwd := Payment{DestAddress: destAddr, PaymentAddress: pymtAddr, TargetBalance: targetBal, PayableBalance: payableBal}
		pymtToDest[pymtAddr] = pymtFwd
	}

	var getCount int
	fmt.Println("Number of payment addresses", len(pymtToDest))

	addrBalExistsStmt, stmtErr := db.Prepare("SELECT paymentAddress, balance, balanceDate FROM AddressBalance WHERE paymentAddress = ?")
	if stmtErr != nil {
		log.Fatal(stmtErr)
		return
	}
	defer addrBalExistsStmt.Close()

	insertBalStmt, insErr := db.Prepare("INSERT INTO AddressBalance (paymentAddress, balance, balanceDate) VALUES (?, ?, datetime('now'))")
	if insErr != nil {
		log.Fatal(insErr)
		return
	}
	defer insertBalStmt.Close()

	updateBalStmt, updErr := db.Prepare("UPDATE AddressBalance SET balance = ?, balance = datetime('now') WHERE paymentAddress = ?")
	if updErr != nil {
		log.Fatal(updErr)
		return
	}
	defer updateBalStmt.Close()

	for key, value := range pymtToDest {
		resp, err := http.Get("https://blockchain.info/address/" + key + "?format=json&limit=10")

		getCount++
		if err != nil {
			log.Fatal(err)
		}

		if resp.StatusCode != 200 {
			fmt.Println("Status Code", resp.StatusCode)
			fmt.Println("Status", resp.Status)
			break
		}

		addressInfo := new(BitcoinAddress)
		readErr := json.NewDecoder(resp.Body).Decode(addressInfo)
		if readErr != nil {
			log.Fatal(readErr)
		}
		resp.Body.Close()

		if addressInfo.Balance > 0 {
			fmt.Println("GET Count", getCount, "Payment address", addressInfo.Address, "Balance", addressInfo.Balance, "Total Received", addressInfo.Received, "Payable", value.PayableBalance)
		}

		existsRows, existsErr := addrBalExistsStmt.Query(addressInfo.Address)
		if existsErr != nil {
			log.Fatal(existsErr)
			break
		}

		var existsCount int
		for existsRows.Next() {
			existsCount++
		}

		if existsCount > 0 {
			updResult, updateErr := updateBalStmt.Exec(addressInfo.Address, addressInfo.Balance)
			if updateErr != nil {
				log.Fatal(updateErr)
				break
			}
			rowsUpdated, _ := updResult.RowsAffected()
			if rowsUpdated < 1 {
				log.Fatal(fmt.Sprintf("Failed to update AddressBalance for address, %s.", addressInfo.Address))
			}
		} else {
			insResult, insErr := insertBalStmt.Exec(addressInfo.Address, addressInfo.Balance)
			if insErr != nil {
				log.Fatal(insErr)
				break
			}
			rowsInserted, _ := insResult.RowsAffected()
			if rowsInserted < 1 {
				log.Fatal(fmt.Sprintf("Failed to insert AddressBalance for address, %s", addressInfo.Address))
			}
		}

	}
	fmt.Println("Total address count:", getCount)
}
