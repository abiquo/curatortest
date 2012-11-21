Run using

````
mvn clean install jetty:run -Djetty.port=9999 -Dzk.serverConnection=localhost:2181
````

You can use the '-Did=identifier' parameter to set the identifier of the Leader candidate (if you care about it)

hit ENTER to redeploy
