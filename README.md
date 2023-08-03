**Coding Challenge - Bitcoin Price**

**Objective:**
Develop an automated and scalable process to obtain the average of each 5 days (moving average) of the price of bitcoin in the first quarter of 2022.

**Brief:**
The finance team needs to analyze the behavior of bitcoin to determine if it is feasible to invest in that currency. Your task is to obtain this information in an automated way and be prepared for sudden changes that must be made at any moment.

**Tasks:**
1. Explore Crypto API
   - Get a list of all coins with id, name, and symbol (using Crypto API)
      - Link: [Crypto API Documentation](https://www.coingecko.com/en/api/documentation)
   - Get bitcoin coin id
   - Get the price of bitcoin in USD and by date of the first quarter of 2022 (using Crypto API)

2. Save the information in the database of your choice

3. Consume the data previously persisted in the database to create a window/partition function for every 5 days (using Spark or Pandas)

4. Add your code to a GitHub repository

**Extra Points:**
- Save the information in the database of your choice (you can do this as part of Task 2)
- Share the GitHub repository link before the interview
- Use the tool of your choice to visualize the results obtained in a graph

Feel free to provide any additional insights, observations, or improvements you make during the process.

Thank you for taking on this coding challenge! Good luck!

**Note:** Make sure to replace "Crypto API" with the specific API you are using for obtaining bitcoin price data. Also, ensure to replace "Spark or Pandas" with the actual technology you decide to use for the window/partition function.
