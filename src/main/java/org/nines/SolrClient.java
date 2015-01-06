package org.nines;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public final class SolrClient {
    
    private String baseUrl;
    private Logger log;
    private MultiThreadedHttpConnectionManager mgr;

    private static final int SOLR_REQUEST_NUM_RETRIES = 5;
    private static final int SOLR_REQUEST_RETRY_INTERVAL = 30 * 1000;
    public static final int HTTP_CLIENT_TIMEOUT = 2 * 60 * 1000; 
    
    public SolrClient(final String baseUrl) {
        
        this.baseUrl = baseUrl;
        this.log = Logger.getLogger(RDFIndexer.class.getName());
        this.mgr = new MultiThreadedHttpConnectionManager( );
        mgr.getParams( ).setDefaultMaxConnectionsPerHost( 5 );
        mgr.getParams( ).setMaxTotalConnections( 5 );
        mgr.getParams( ).setConnectionTimeout( HTTP_CLIENT_TIMEOUT );
        mgr.getParams( ).setIntParameter( HttpMethodParams.BUFFER_WARN_TRIGGER_LIMIT, 10000 * 1024 );
    }
    
    private HttpClient newHttpClient( ) {
        //mgr.deleteClosedConnections( );
        return( new HttpClient( mgr ) );
    }
    
    /**
     * Check if core exists. Create it if it does not
     * @param core
     */
    public void validateCore( final String core ) throws IOException {

        GetMethod request = null;
        try {
            request = new GetMethod( this.baseUrl+"/admin/cores?action=STATUS");
            execRequest( request );
            String response = getResponseString( request );
            int exists = response.indexOf(">" + core + "<");
            if (exists <= 0) {
                String instanceDir = "archives";
                if (core.indexOf("pages_") == 0) {
                    instanceDir = "pages";
                }
                request = new GetMethod(this.baseUrl+"/admin/cores?action=CREATE&name=" 
                    + core + "&instanceDir="+instanceDir+"&dataDir=" + core);

                execRequest( request );
                getResponseString( request );

                this.log.info(">>>> Created core: " + core);
            }
        } catch (IOException e ){
            throw e;
        } finally {
            if ( request != null ) {
                request.releaseConnection();
            }
        }
    }
    
    private final void execRequest( HttpMethod request ) throws IOException {
        
        HttpClient httpClient = newHttpClient();
        int responseCode;
        int solrRequestNumRetries = SOLR_REQUEST_NUM_RETRIES;
        do {
            responseCode = httpClient.executeMethod(request);

            if (responseCode != 200) {
                try {
                    Thread.sleep(SOLR_REQUEST_RETRY_INTERVAL);
                    log.info(">>>> SOLR request "+request.getURI( ).toString( )+" FAILED : "
                        + responseCode + " (retrying...)");
                } catch (InterruptedException e) {}
            } else {
                if (solrRequestNumRetries != SOLR_REQUEST_NUM_RETRIES) {
                    log.info(">>>> SOLR request "+request.getURI( ).toString( )+":  (succeeded!)");
                }
            }
            solrRequestNumRetries--;
        } while (responseCode != 200 && solrRequestNumRetries > 0);
        
        if (responseCode != 200) {
            throw new IOException("Non-OK response: " + responseCode + "\n\n" + request.getResponseBodyAsString() );
        }
    }
    
    private final String getResponseString(HttpMethod httpMethod) throws IOException {
        InputStream is = httpMethod.getResponseBodyAsStream( );
        if( is != null ) {
            String s = IOUtils.toString( is, "UTF-8" );
            is.close();
            return ( s );
        }
        return( "" );
    }
    
    public final List<JsonObject> getResultsPage( final String core, final String archive,
        final int page, final int pageSize, final String fields, final List<String> andConstraints, final List<String> orConstraints )  {

        ArrayList<JsonObject> result = new ArrayList<JsonObject>();
        GetMethod get;

        // never request the _version_ field
        String filtered_fields = fields.replace("_version_", "");
        
        // build the request query string
        try {
            String a = URLEncoder.encode("\"" + archive + "\"", "UTF-8");
            String query = this.baseUrl + "/" + core + "/select/?q=archive:" + a;
            query += "&start=" + (page * pageSize) + "&rows=" + pageSize;
            query += "&fl=" + filtered_fields;
            query += "&sort=uri+asc";
            query += "&wt=json";

            // add the constraints as necessary...
            String constraints = "";
            boolean first = true;
            if( andConstraints != null && andConstraints.isEmpty( ) == false ) {

                for( String constraint : andConstraints ) {
                    String [] tokens = constraint.split( "=", 2 );
                    if( first == false ) constraints += "+AND+";
                    constraints += tokens[ 0 ] + URLEncoder.encode( ":", "UTF-8" ) + tokens[ 1 ];
                    first = false;
                }
            } else if( orConstraints != null && orConstraints.isEmpty( ) == false ) {
               for( String constraint : orConstraints ) {
                  String [] tokens = constraint.split( "=", 2 );
                  if( first == false ) constraints += "+OR+";
                  constraints += tokens[ 0 ] + URLEncoder.encode( ":", "UTF-8" ) + tokens[ 1 ];
                  first = false;
               }
            }

            if( constraints.isEmpty( ) == false ) query += "&fq=" + constraints;

            //System.out.println("*** SOLR QUERY: " + query );

            get = new GetMethod(query);
        } catch (UnsupportedEncodingException e) {
            this.log.error("Unable to create SOLR request query", e);
            return result;
        }

        // execute the query
        try {
            execRequest(get);
        } catch (IOException e) {
            this.log.error("SOLR request failed", e);
            get.releaseConnection();
            return result;
        }

        // read the result into an array of JSON objects
        try  {
            JsonParser parser = new JsonParser();
            String res = getResponseString( get );
            JsonElement parsed = parser.parse( res );
            JsonObject data = parsed.getAsJsonObject();
            JsonObject re = data.get( "response" ).getAsJsonObject();
            JsonElement de = re.get( "docs" );
            JsonArray docs = de.getAsJsonArray();
            Iterator<JsonElement> i = docs.iterator();
            while( i.hasNext() ) {
                result.add( i.next().getAsJsonObject() );
            }
        } catch (IOException e ) {
            this.log.error("Unable to read SOLR response", e);
        } finally {
            get.releaseConnection( );
        }
        return result;
    }
    
    /**
     * Post the JSON payload to the specified SOLR archive
     * 
     * @param json
     * @param archive
     * @throws IOException
     */
    public void postJSON(String json, String archive) throws IOException {

        PostMethod post = new PostMethod(this.baseUrl + "/" + archive + "/update/json");
        post.setRequestEntity(new StringRequestEntity(json, "application/json", "utf-8"));
        post.setRequestHeader("Content-type", "application/json; charset=utf-8");

        // Execute request
        try {
            execRequest( post );
            String response = getResponseString( post );
            Pattern pattern = Pattern.compile( "status=\\\"(\\d*)\\\">(.*)\\<\\/result\\>", Pattern.DOTALL );
            Matcher matcher = pattern.matcher( response );
            while( matcher.find() ) {
                String status = matcher.group( 1 );
                String message = matcher.group( 2 );
                if( !"0".equals( status ) ) {
                    throw new IOException( message );
                }
            }
        } catch( IOException ex ) {
            this.log.error( "SOLR request failed: ", ex);
            this.log.error( "REQUEST: " + json );
        } finally {
            // Release current connection to the connection pool once you are done
            post.releaseConnection();
        }
    }

    public void commit( String archive ) {
        try {
            postJSON("{\"commit\": {}}", archive );
        } catch (IOException e) {
            this.log.error("Commit to SOLR FAILED: " + e.getMessage());
        }
    }

}
