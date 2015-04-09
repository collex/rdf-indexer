package org.nines;

import java.util.concurrent.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import java.io.IOException;


@SuppressWarnings("rawtypes")
public class AsyncPoster {

    private ExecutorService service = null;
    private List<Future> pending = null;
    private Logger log = Logger.getLogger( AsyncPoster.class );

    public AsyncPoster( final int concurrent ) {
        this.service = Executors.newFixedThreadPool( concurrent );
        pending = new ArrayList<Future>( );
    }

    public void shutdown( ) {
        log.info( "Shutting down..." );
        // signal shutdown and wait until it is complete
        this.service.shutdown( );
        try {
            this.service.awaitTermination( 15, TimeUnit.MINUTES );
        } catch (InterruptedException e) {
            // do nothing...
        }
    }

    public void asyncPost( final SolrClient client, final String archive, final String payload ) {
        removeDone( );
        pending.add( this.service.submit( new SolrPoster( client, payload, archive ) ) );
    }

    public void asyncCommit( final SolrClient client, final String archive ) {
        removeDone( );
        pending.add( this.service.submit( new SolrCommitter( client, archive ) ) );
    }

    // wait for any pending tasks to complete
    public void waitForPending( ) {

        log.info( "Waiting for pending tasks..." );
        while( pending.isEmpty( ) == false ) {
            try {
                pending.get( 0 ).get( );
                pending.remove( 0 );
            } catch( InterruptedException ex ) {
                // do nothing...
            } catch( ExecutionException ex ) {
                // do nothing...
            }
        }
        log.info( "All pending tasks complete" );
    }

    public void removeDone( ) {
       for( Iterator<Future> f = pending.iterator( ); f.hasNext( ); ) {
          if( f.next( ).isDone( ) == true ) {
              f.remove( );
          }
       }
    }

    // Worker thread to post data to solr
    private class SolrPoster implements Runnable {

        private final SolrClient client;
        private final String payload;
        private final String archive;

        public SolrPoster( final SolrClient client, final String payload, final String archive ) {
            this.client = client;
            this.archive = archive;
            this.payload = payload;

            log.info( "  posting: payload size " + this.payload.length( ) + " to SOLR archive " + this.archive );
        }

        public void run( ) {
            try {
                client.postJSON( this.payload, this.archive );
            } catch( IOException ex ) {
                log.error( "Post to SOLR FAILED: " + ex.getMessage( ) );
                ex.printStackTrace( );
            }
        }
    }

    // Worker thread to commit data to solr
    private class SolrCommitter implements Runnable {

        private final SolrClient client;
        private final String archive;

        public SolrCommitter( final SolrClient client, final String archive ) {
            this.client = client;
            this.archive = archive;
            log.info("  committing to SOLR archive " + archive );
        }

        public void run( ) {
            client.commit( this.archive );
        }
    }
}
