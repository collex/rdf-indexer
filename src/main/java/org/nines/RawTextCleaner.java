package org.nines;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Cleaner for Raw text files. It will clean out unused tags,
 * fix escape sequences and strip bad utf-8 characters. Errors
 * and changes will be written out to the log files. The cleaned file
 * will be written out to the fullltext area of solr sources
 * 
 * @author loufoster
 *
 */
public class RawTextCleaner {

    private CharsetDecoder decoder;
    private ErrorReport errorReport;    
    private RDFIndexerConfig config;
    private Logger log;
    private long totalOrigChars = 0;
    private long totalFilesChanged = 0;
    private long totalCleanedChars = 0;
    
    public RawTextCleaner( RDFIndexerConfig config, ErrorReport errorReport ) {
        this.errorReport = errorReport;
        this.config = config;
        this.log = Logger.getLogger(RawTextCleaner.class.getName());
                
        Charset cs = Charset.availableCharsets().get("UTF-8");
        this.decoder = cs.newDecoder();
        this.decoder.onMalformedInput(CodingErrorAction.REPLACE);
        this.decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
    
    /**
     * Clean thespecifled file and write the results to the fulltext folder.
     * Errors will be added to the <code>errorReport</code>
     * 
     * @param rawTextFile
     * @param errorReport
     */
    public void clean( final File rawTextFile ) {
    
        this.log.info("Clean raw text from file "+rawTextFile);
        
        // Read the raw text from the file. Bail if this fails
        String content = null;
        InputStreamReader is = null;
        try {
            is = new InputStreamReader(new FileInputStream(rawTextFile), this.decoder);
            content =  IOUtils.toString(is);
        } catch ( Exception e ) {
            this.errorReport.addError( 
                new IndexerError(rawTextFile.toString(), "", "Unable to read raw text file: " + e.toString()));
            return;
            
        } finally {
            IOUtils.closeQuietly(is);
        }
        
        // stats!
        long startChars = content.length();;
        this.totalOrigChars += startChars;
        
        // clean it up as best as possible
        content = TextUtils.stripUnknownUTF8(content, this.errorReport, rawTextFile);
        content = TextUtils.stripEscapeSequences(content, errorReport, rawTextFile); 
        content = cleanText( content );
        
        long endChars = content.length();
        this.totalCleanedChars += ( startChars - endChars);
        if ( endChars != startChars ) {
            this.totalFilesChanged++;
        }
        this.log.info("  => Original length: "+startChars+", Cleaned length: "+endChars+", Delta:"+(startChars - endChars) );
        
        // get the filename for the cleaned fulltext file
        File out = toFullTextFile(rawTextFile);
        
        // Make sure that the directory structure exists
        if ( out.getParentFile().exists() == false) {
            if ( out.getParentFile().mkdirs() == false ) {
                this.errorReport.addError(
                    new IndexerError(out.toString(), "", "Unable to create full text directory tree"));
                return;
            }
        }
        
        // dump the content
        FileWriter fw = null;
        try {
            fw = new FileWriter(out);
            fw.write(content);
        } catch (IOException e) {
            this.errorReport.addError( 
                new IndexerError(out.toString(), "", "Unable to write cleaned text file: " + e.toString()));
        } finally {
            IOUtils.closeQuietly(fw);
        }
    }
    
    private File toFullTextFile(File rawTextFile) {
        String cleanedFile = this.config.sourceDir.toString().replace("rawtext", "fulltext") 
            + "/" + SolrClient.safeCore(this.config.archiveName);
        return new File(cleanedFile +"/" + rawTextFile.getName());   
    }
    
    public long getTotalFilesChanged() {
        return this.totalFilesChanged;
    }
    
    public long getOriginalLength() {
        return this.totalOrigChars;
    }
    
    public long getCleanedLength() {
        return this.totalCleanedChars;
    }
    
    /**
     * Strip html-ish markup from text
     * @param fullText
     * @return
     */
    private String cleanText( String fullText ) {

        // remove everything between <head>...</head>
        fullText = removeTag(fullText, "head");

        // remove everything between <script>..</script>
        fullText = removeTag(fullText, "script");

        // remove everything between <...>
        fullText = removeBracketed(fullText, "<", ">");

        // Get rid of non-unix line endings
        fullText = fullText.replaceAll("\r", "");

        // Clean up the file a little bit -- there shouldn't be two spaces in a row or blank lines
        fullText = fullText.replaceAll("&nbsp;", " ");
        fullText = fullText.replaceAll("&#160;", " ");
        fullText = fullText.replaceAll("\t", " ");
        fullText = fullText.replaceAll(" +", " ");
        fullText = fullText.replaceAll(" \n", "\n");
        fullText = fullText.replaceAll("\n ", "\n");
        fullText = fullText.replaceAll("\n+", "\n");

        return fullText;
    }
    
    private String removeBracketed(String fullText, String left, String right) {
        int start = fullText.indexOf(left);
        while (start != -1) {
            int end = fullText.indexOf(right, start);
            if (end == -1) {
                start = -1;
            } else {
                fullText = fullText.substring(0, start) + "\n" + fullText.substring(end + right.length());
                start = fullText.indexOf(left);
            }
        }
        return fullText;
    }

    private String removeTag(String fullText, String tag) {
        return removeBracketed(fullText, "<" + tag, "</" + tag + ">");
    }

}
