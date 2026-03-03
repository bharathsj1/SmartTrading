from ib_insync import *

ib = IB()
ib.connect("127.0.0.1", 4002, clientId=11)  # your TWS paper port

contract = Forex("EURUSD")
ib.qualifyContracts(contract)

# Get a usable price first
ib.reqMarketDataType(3)  # delayed is fine for testing
ticker = ib.reqMktData(contract, "", False, False)

px = None
for _ in range(40):
    ib.sleep(0.5)
    px = ticker.marketPrice()
    if px and px > 0:
        break

if not px:
    print("No price, cannot proceed.")
    ib.disconnect()
    raise SystemExit(1)

print("Price:", px)

# Choose position size (EUR amount)
qty = 10000  # 10k EUR (you can change)

# For FX, a BUY EURUSD means buy EUR / sell USD
# Place a LIMIT BUY slightly ABOVE market to increase fill chance
limit_price = round(px * 1.0002, 5)  # +0.02% (FX uses 5 decimals often)

order = LimitOrder("BUY", qty, limit_price)
order.tif = "DAY"

trade = ib.placeOrder(contract, order)

while not trade.isDone():
    ib.sleep(0.5)
    print("Status:", trade.orderStatus.status)

print("FINAL:", trade.orderStatus.status,
      "| Filled:", trade.orderStatus.filled,
      "| Avg:", trade.orderStatus.avgFillPrice)

# Print logs if it fails
print("AdvancedError:", trade.advancedError)
for entry in trade.log:
    print(entry.time, entry.status, entry.message, entry.errorCode)

ib.cancelMktData(contract)
ib.disconnect()