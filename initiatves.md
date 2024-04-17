### TODO

1. Improve groups further
2. create the prometheus metrics
2. Use channel

1. App is not resialiant to failed writes do DB. What to do then?
2. Agglomerate all the operations in the same function (between store and window) in a single batch. (will help with previous problem)
3. Send the BD batch to a channel, then write it synchronously and then ack


