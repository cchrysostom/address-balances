select PaymentAddress.destinationAddress, PaymentAddress.paymentAddress, PaymentAddress.targetBalance, PaymentAddress.payableBalance,
       PaymentAddress.status, PaymentAddress.forwarded, AddressBalance.balance, AddressBalance.balanceDate
  from PaymentAddress join AddressBalance using (paymentAddress)
  where AddressBalance.balance > 0
  order by PaymentAddress.destinationAddress, AddressBalance.balance desc
  