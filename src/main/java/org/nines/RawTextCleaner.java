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
import org.apache.commons.lang.StringEscapeUtils;

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
    
    public RawTextCleaner( RDFIndexerConfig config, ErrorReport errorReport ) {
        this.errorReport = errorReport;
        this.config = config;
        
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
    
        // Read the raw text from the file. Bail if this fails
        String content = null;
        InputStreamReader is = null;
        try {
            is = new InputStreamReader(new FileInputStream(rawTextFile), this.decoder);
            content =  IOUtils.toString(is);
        } catch ( Exception e ) {
            this.errorReport.addError( 
                new IndexerError(rawTextFile.toString(), "", "Unable to read rw text file: " + e.toString()));
            return;
            
        } finally {
            IOUtils.closeQuietly(is);
        }
        
        // clean it up as best as possible
        content = stripUnknownUTF8(rawTextFile.getName(), content);
        content = cleanText( content );
        content = StringEscapeUtils.unescapeXml(content);        
        content = stripBadEscapeSequences(rawTextFile.getName(), content);
        
        // get the filename for the cleaned fulltext file
        String cleanedFile = this.config.getFullTextRoot() + SolrClient.safeCore(this.config.archiveName);
        File out = new File(cleanedFile +"/" + rawTextFile.getName());
        
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
                new IndexerError(cleanedFile.toString(), "", "Unable to write cleaned text file: " + e.toString()));
        } finally {
            IOUtils.closeQuietly(fw);
        }
    }
    
    /**
     * Remove unknown UTF-8 characters (0xFFFD) and log warnings for each
     * @param value
     * @return
     */
    private String stripUnknownUTF8( final String fileName, final String value) {
        // Look for unknown character and warn
        int curPos= 0;
        while ( true ) {
            int pos = value.indexOf("\ufffd", curPos);
            if (pos == -1) {
                break;
            }
            curPos = pos+1;
            
            String snip = value.substring(Math.max(0, pos-25), Math.min(value.length(), pos+25));            
            errorReport.addError(new IndexerError(fileName, "", 
                    "Invalid UTF-8 character at position " + pos
                    + " of field text"
                    + "\n  Snippet: ["+snip+"]"));
                
        }
        return value.replaceAll("\ufffd", "");
    }
    
    /**
     * Find any occurances of &# with a ; within 6 chars strip them out and flag as a potential error.
     * Only call this on text that has been fully unescaped.
     * 
     * @param text
     * @return
     */
    protected String stripBadEscapeSequences(final String fileName, String text) {

        int startPos = 0;
        while (true) {
            int pos = text.indexOf("&#", startPos);
            if (pos == -1) {
                break;
            } else {
                // look for a trainling ; to end the sequence
                int pos2 = text.indexOf(";", pos);
                if (pos2 > -1) {
                    // this is likely an escape sequence
                    if (pos2 <= pos + 6) {

                        // dump the bad sequence
                        String bad = text.substring(pos, pos2 + 1);
                        String snip = text.substring(Math.max(0, pos-25), Math.min(text.length(), pos+25));           
                        text = text.replaceAll(bad, "");
                            errorReport.addError(new IndexerError(fileName, "",
                                    "Removed potentially invalid escape sequece [" + bad 
                                    + "]\nSnippet: ["+snip+"]"));
                            startPos = pos;

                    } else {

                        // no close ; found. Just skip over the &#
                        startPos = pos + 2;
                    }

                } else {
                    // NO ; found - skip over the &#
                    startPos = pos + 2;
                }
            }
        }
        return text;
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
