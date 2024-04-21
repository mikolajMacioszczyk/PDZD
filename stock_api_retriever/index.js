const fs = require('fs');

const API_KEYS = [
    'aeEWz3omkERQOmfr3DBdANZCQgS79LQ8',
    '4j7TQjKWWOZk18vXCfhWaepQ25kM4wOE',
    'fGQeKxS1N2B4WeNFw3F17vgWiltOiKzj',
    'LJp8OVulOKYflSmpBfqOGzpcGXLqoBzh',
    'jGQ8n9JcBCaCLdOK9fty1RqmX68ECRIW',
]
const COMPANY_INFO_ENDPOINT = 'https://financialmodelingprep.com/api/v3/profile/';
const COMPANY_STOCK_DATA_ENDPOINT = 'https://financialmodelingprep.com/api/v3/historical-price-full/';

fs.readFile('stocklist.json', 'utf8', (err, stockData) => {
    if (err) {
        console.error(err);
        return;
    }
    const stocks = JSON.parse(stockData);

    fs.readFile('smp500.json', 'utf8', async (err, smpData) => {
        if (err) {
            console.error(err);
            return;
        }
        const smpSymbols = JSON.parse(smpData);

        const filteredStocks = stocks.filter(stock => smpSymbols.companies.map(({symbol}) => symbol).includes(stock.symbol));

        const stocksWithInfo = await Promise.all(filteredStocks.map(async (stock, index) => {
            const apiKey = API_KEYS[index % API_KEYS.length];
            const companyInfo = await fetch(`${COMPANY_INFO_ENDPOINT}${stock.symbol}?apikey=${apiKey}`)
            const stockData = await fetch(`${COMPANY_STOCK_DATA_ENDPOINT}${stock.symbol}?from=2017-01-01&to=2023-01-01&apikey=${apiKey}`)
  
            return {...filteredStocks[index], ...(await companyInfo.json())[0], stockData: (await stockData.json()).historical}
  
        }));
 
        fs.writeFile('stockdata.json', JSON.stringify(stocksWithInfo, null, 2), (err) => {
            if (err) {
                console.error(err);
                return;
            }
            console.log('Filtered stocks written to filteredStocks2.json');
        });
    });
});
