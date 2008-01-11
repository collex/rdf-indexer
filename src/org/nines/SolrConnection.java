package org.nines;

import java.io.IOException;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;

/**
 * SolrConnection
 * @author nicklaiacona
 */
public class SolrConnection {

    private static final int SOLR_REQUEST_NUM_RETRIES = 5; // how many times we should try to connect with solr before giving up
    private static final int SOLR_REQUEST_RETRY_INTERVAL = 30 * 1000; // milliseconds
    public static final int HTTP_CLIENT_TIMEOUT = 2 * 60 * 1000; // 2 minutes

    private String solrURL;
    private HttpClient httpclient;
    
    public static void main( String args[] ) {
        SolrConnection connection = new SolrConnection("http://localhost:8983/solr");
        long startTime = System.currentTimeMillis();
        try {
            System.out.print(connection.querySolr("q=books"));
        }
        catch( IOException e ) {
            System.out.print("An error occurred.");
        }
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.print("That took: "+elapsedTime+" milliseconds.");
                
    }
            
    public SolrConnection(String solrURL) {
        httpclient = new HttpClient();
        this.solrURL = solrURL;
    }

    public String querySolr(String queryParams) throws IOException {
        GetMethod get = new GetMethod(solrURL+"/select?"+queryParams);
        get.setRequestHeader("Content-type", "text/xml; charset=utf-8");

        httpclient.getHttpConnectionManager().getParams().setConnectionTimeout(HTTP_CLIENT_TIMEOUT);
        // Execute request
        int result;
        int solrRequestNumRetries = SOLR_REQUEST_NUM_RETRIES;
        do {
            result = httpclient.executeMethod(get);
            solrRequestNumRetries--;
            if (result != 200) {
                try {
                    Thread.sleep(SOLR_REQUEST_RETRY_INTERVAL);
                } catch (InterruptedException e) {

                }
            }
        } while (result != 200 && solrRequestNumRetries > 0);

        if (result != 200) {
            throw new IOException("Non-OK response: " + result + "\n\n");
        }

        return get.getResponseBodyAsString();
    }
}
