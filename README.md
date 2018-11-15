# sparkRedisPythonExample
python application to read csv file into spark dataframe and save into redis. And then test saved data by queries.

# dependencies
install apache spark and hadoop
run redis-server with default port

# steps to run
open terminal and run redisExample.py
Wait until message shows "APPLICATION IS READY. RUN CURL COMMAND TO TEST"
open another terminal and run following curl command
curl -H 'Content-Type: application/json' "http://127.0.0.1:5002/getItemsbyColor/SilVER"
curl -H 'Content-Type: application/json' "http://127.0.0.1:5002/getRecentItem/2016-02-18T20:57:42Z"
curl -H 'Content-Type: application/json' "http://127.0.0.1:5002/getBrandsCount"
