### Business Scenario Description

The necessity is to simulate multiple scenarios to predict the possibilities of losses in the management of a logistics distribution center with with hundreds of thousands of different products(SKU's) in stock.

---

Updates may continually occur that affect simulation inputs, e.g:
  - scheduling for product withdrawals from stock
  - withdrawal of products from stock, previously scheduled
  - scheduling for entry/delivery of new products into stock
  - entries/deliveries of new products, previously scheduled

In this case new simulations are needed to update the forecast scenarios.

---

The results of simulations must to show, day by day for the next 90 days, the probability of happen 3 kinds of losses:
- scheduled entries/deliveries that will not have free space in the warehouse
- product(sku) requested to withdraw but missing in the stock
- product(sku) that will achieve the expiration date in the stock

---

#### Monte Carlo Simulation Scenarios

> The simulations will not be based only on knowledge about scheduled entries and withdrawals. But also in supply and demand forecasts based on historical data from the last 5 years and random shocks. Forming multiple ***Monte Carlo Simulation*** scenarios.

The scenarios will be distributed equally across 9 categories:
   - supply on an uptrend, demand stable
   - supply on an uptrend, demand on a downtrend
   - supply on an uptrend, demand on an uptrend
   - supply on a downtrend, demand stable
   - supply on a downtrend, demand on a downtrend
   - supply on a downtrend, demand on an uptrend
   - stable supply, stable demand 
   - stable supply, demand on a downtrend
   - stable supply, demand on an uptrend

The trend will be represented by a multiply factor over the historical referential data, e.g (uptrend of 20% will be the multiply factor: 1.2)




---
---

### Architecture Definition