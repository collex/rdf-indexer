/** 
 *  Copyright 2007 Applied Research in Patacriticism and the University of Virginia
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 **/
package org.nines;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.nines.RDFIndexerConfig.Mode;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

final class NinesStatementHandler implements RDFHandler {
    private final static Logger log = Logger.getLogger(NinesStatementHandler.class.getName());

    private HashMap<String, HashMap<String, ArrayList<String>>> documents;
    private String dateBNodeId;
    private HashMap<String, ArrayList<String>> doc;
    private Boolean title_sort_added = false;
    private File file;
    private RDFIndexerConfig config;
    private ErrorReport errorReport;
    private String documentURI;
    private long largestTextField = -1;
    private LinkCollector linkCollector;
    private boolean hasCorrectedText = false;

    private static String uncertain = "Uncertain";

    public NinesStatementHandler(ErrorReport errorReport, LinkCollector linkCollector, RDFIndexerConfig config) {
        this.errorReport = errorReport;
        this.config = config;
        doc = new HashMap<String, ArrayList<String>>();
        documentURI = "";
        documents = new HashMap<String, HashMap<String, ArrayList<String>>>();
        this.linkCollector = linkCollector;
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
        System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
        System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.commons.httpclient", "error");
    }

    public void handleStatement(Statement statement) throws RDFHandlerException {

        String subject = statement.getSubject().stringValue();
        String predicate = statement.getPredicate().stringValue();
        String object = statement.getObject().stringValue();
        
        // if the object of the triple is blank, skip it, it is nothing worth indexing
        // EXCEPT for text in page-level RDF. There are valid cases where the collex:text
        // for a page is blank. To avoid streaming out validation errors on these
        // cases, let blanks through. There is matching code in handleText.
        if ( object == null || object.length() == 0 ) {
            if ( !(this.config.isPagesArchive() && "http://www.collex.org/schema#text".equals(predicate)) ) {
                return;
            }
        }

        // start of a new document
        if ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type".equals(predicate)
            && statement.getSubject() instanceof URIImpl) {
            if (documents.get(subject) != null) {
                errorReport.addError(new IndexerError(this.file.toString(), subject, "Duplicate URI"));
                log.info("*** Duplicate: " + subject);
            }
            doc = new HashMap<String, ArrayList<String>>();
            addField(doc, "uri", subject);
            documents.put(subject, doc);
            title_sort_added = false;
            documentURI = subject;
            this.hasCorrectedText = ( this.config.correctedTextMap.containsKey(this.documentURI));
            log.info("Parsing RDF for document: " + subject);
            errorReport.flush();
        }
        
        // Check for any unsupported nines:* attributes and issue error if any exist
        if (predicate.startsWith("http://www.nines.org/schema#")) {
            addError( "NINES is no longer a valid attribute: "+ predicate);
            return;
        }

        if (predicate.startsWith("http://www.collex.org/schema#")) {
            String attribute = predicate.substring("http://www.collex.org/schema#".length());
            if (!(attribute.equals("archive") || attribute.equals("freeculture") || attribute.equals("source_xml")
                || attribute.equals("source_html") || attribute.equals("source_sgml") || attribute.equals("federation")
                || attribute.equals("ocr") || attribute.equals("genre") || attribute.equals("thumbnail")
                || attribute.equals("text") || attribute.equals("fulltext") || attribute.equals("image")
                || attribute.equals("pages") || attribute.equals("pagenum") || attribute.equals("pageof") 
                || attribute.equals("discipline") || attribute.equals("typewright"))) {

                addError("Collex does not support this property: " + predicate );
                return;
            }
        }

        // parse RDF statements into fields, return when the statement has been handled
        if (handleFederation(predicate, object))
            return;
        if (handleOcr(predicate, object))
            return;
        if (handlePages(predicate, object))
            return;
        if (handlePageNum(predicate, object))
            return;
        if (handlePageOf(predicate, object))
            return;
        if (handleTypewright(predicate, object))
            return;
        if (handleFullText(predicate, object))
            return;
        if (handleCollexSourceXml(predicate, object))
            return;
        if (handleCollexSourceHtml(predicate, object))
            return;
        if (handleCollexSourceSgml(predicate, object))
            return;
        if (handleArchive(predicate, object))
            return;
        if (handleFreeCulture(predicate, object))
            return;
        if (handleTitle(predicate, object))
            return;
        if (handleAlternative(predicate, object))
            return;
        if (handleGenre(predicate, object))
            return;
        if (handleDate(subject, predicate, statement.getObject()))
            return;
        if (handleDateLabel(subject, predicate, object))
            return;
        if (handleSource(predicate, object))
            return;
        if (handleThumbnail(predicate, object))
            return;
        if (handleImage(predicate, object))
            return;
        if (handleURL(predicate, object))
            return;
        if (handleText(predicate, object))
            return;
        if (handleRole(predicate, object))
            return;
        if (handlePerson(predicate, object))
            return;
        if (handleFormat(predicate, object))
            return;
        if (handleLanguage(predicate, object))
            return;
        if (handleGeospacial(predicate, object))
            return;
        if (handleProvenance(predicate, object))
            return;
        if (handleDiscipline(predicate, object))
            return;
        if (handleSubject(predicate, object))
            return;
        if (handleType(predicate, object))
            return;
        if (handleHasPart(predicate, object))
            return;
        if (handleIsPartOf(predicate, object))
            return;
    }

    private boolean handleFederation(String predicate, String object) {
        if ("http://www.collex.org/schema#federation".equals(predicate)) {
            if (object.equals("NINES") || object.equals("18thConnect") || object.equals("MESA") || 
                object.equals("ModNets") || object.equals("SiRO") || object.equals("estc") || object.equals("GLA") ) {
                addField(doc, "federation", object);
            } else {
                addError("Unknown federation: " + object);
            }
            return true;
        }
        return false;
    }

    private boolean handleOcr(String predicate, String object) {
        if ("http://www.collex.org/schema#ocr".equals(predicate)) {
            if ("true".equalsIgnoreCase(object)) {
                // only add a ocr field if it's true. No field set implies "F"alse
                addField(doc, "is_ocr", "T");
                return true;
            }
        }
        return false;
    }

    private boolean handleTypewright(String predicate, String object) {
        if ("http://www.collex.org/schema#typewright".equals(predicate)) {
            if ("true".equalsIgnoreCase(object)) {
                // only add a typewright field if it's true. No field set implies "F"alse
                addField(doc, "typewright", "T");
                return true;
            }
        }
        return false;
    }

    private boolean handlePerson(String predicate, String object) {
        if ("http://www.collex.org/schema#person".equals(predicate)) {
            addField(doc, "person", object);
            return true;
        }
        return false;
    }

    private boolean handleFormat(String predicate, String object) {
        if ("http://purl.org/dc/elements/1.1/format".equals(predicate)) {
            addField(doc, "format", object);
            return true;
        }
        return false;
    }

    private boolean handleLanguage(String predicate, String object) {
        if ("http://purl.org/dc/elements/1.1/language".equals(predicate)) {
            addField(doc, "language", object);
            return true;
        }
        return false;
    }

    private boolean handleGeospacial(String predicate, String object) {
        if ("http://www.collex.org/schema#geospacial".equals(predicate)) {
            addField(doc, "geospacial", object);
            return true;
        }
        return false;
    }

    private boolean handleCollexSourceXml(String predicate, String object) {
        if ("http://www.collex.org/schema#source_xml".equals(predicate)) {
            addField(doc, "source_xml", object);
            return true;
        }
        return false;
    }

    private boolean handleCollexSourceHtml(String predicate, String object) {
        if ("http://www.collex.org/schema#source_html".equals(predicate)) {
            addField(doc, "source_html", object);
            return true;
        }
        return false;
    }

    private boolean handleCollexSourceSgml(String predicate, String object) {
        if ("http://www.collex.org/schema#source_sgml".equals(predicate)) {
            addField(doc, "source_sgml", object);
            return true;
        }
        return false;
    }

    private boolean handleArchive(String predicate, String object) {
        if ("http://www.collex.org/schema#archive".equals(predicate)) {
            addField(doc, "archive", object);
            return true;
        }
        return false;
    }

    private boolean handleFreeCulture(String predicate, String object) {
        if ("http://www.collex.org/schema#freeculture".equals(predicate)) {
            if ("false".equalsIgnoreCase(object)) {
                addFieldEntry(doc, "freeculture", "F", true); // "F"alse
            } else if ("true".equalsIgnoreCase(object)) {
                addFieldEntry(doc, "freeculture", "T", true); // "T"rue
            }
            return true;
        }
        return false;
    }
    
    private boolean handlePages(String predicate, String object) {
        if ("http://www.collex.org/schema#pages".equals(predicate)) {
            if ("false".equalsIgnoreCase(object)) {
                addFieldEntry(doc, "has_pages", "F", true); // "F"alse
            } else if ("true".equalsIgnoreCase(object)) {
                addFieldEntry(doc, "has_pages", "T", true); // "T"rue
            }
            return true;
        }
        return false;
    }
    
    private boolean handlePageOf(String predicate, String object) {
        if ("http://www.collex.org/schema#pageof".equals(predicate)) {
            addField(doc, "page_of", object);
            return true;
        }
        return false;
    }
    
    private boolean handlePageNum(String predicate, String object) {
        if ("http://www.collex.org/schema#pagenum".equals(predicate)) {
            addField(doc, "page_num", object);
            return true;
        }
        return false;
    }

    private boolean handleFullText(String predicate, String object) {
        if ("http://www.collex.org/schema#fulltext".equals(predicate)) {
            if ( this.hasCorrectedText ) {
                addField(doc, "has_full_text", "T"); 
            } else {
                if ("false".equalsIgnoreCase(object)) {
                    // only add a fulltext field if its false. No field set implies "T"rue
                    addField(doc, "has_full_text", "F"); // "F"alse
                }
            }
            return true;
        }
        return false;
    }

    private boolean handleTitle(String predicate, String object) {
        if ("http://purl.org/dc/elements/1.1/title".equals(predicate)) {
            addField(doc, "title", object);
            if (!title_sort_added) {
                addField(doc, "title_sort", object);
                title_sort_added = true;
            }
            return true;
        }
        return false;
    }

    private boolean handleProvenance(String predicate, String object) {
        if ("http://purl.org/dc/elements/1.1/provenance".equals(predicate)) {
            addField(doc, "provenance", object);
            return true;
        }
        return false;
    }

    private boolean handleType(String predicate, String object) {
        if ("http://purl.org/dc/elements/1.1/type".equals(predicate)) {
            addField(doc, "doc_type", object);
            return true;
        }
        return false;
    }

    private boolean handleDiscipline(String predicate, String object) {
            if ("http://www.collex.org/schema#discipline".equals(predicate)) {
                addField(doc, "discipline", object);
                return true;
            }
            return false;
        }

    private boolean handleSubject(String predicate, String object) {
        if ("http://purl.org/dc/elements/1.1/subject".equals(predicate)) {
            addField(doc, "subject", object);
            return true;
        }
        return false;
    }

    private boolean handleAlternative(String predicate, String object) {
        if ("http://purl.org/dc/terms/alternative".equals(predicate)) {
            addField(doc, "alternative", object);
            return true;
        }
        return false;
    }

    private boolean handleGenre(String predicate, String object) {
        if ("http://www.collex.org/schema#genre".equals(predicate)) {
            // ignore deprecated genres for backward compatibility
            if (!"Primary".equals(object) && !"Secondary".equals(object)) {
                addField(doc, "genre", object);
            }
            return true;
        }
        return false;
    }

    private boolean handleDate(String subject, String predicate, Value value) {
        if ("http://purl.org/dc/elements/1.1/date".equals(predicate)) {
            String object = value.stringValue().trim();
            if (value instanceof LiteralImpl) {

                // add label
                addField(doc, "date_label", object);

                //System.out.println( "handleDate: " + object );

                ArrayList<String> years = parseYears(object);

                if( years.isEmpty() == true ) {
                    addError("Invalid date format: " + object);
                    return false;
                }

                // add the years
                for (String year : years) {
                    addFieldIfUnique(doc, "year", year);
                }

                // and any fields that are derived from the years
                addDerivedDateFields( years );
            } else {
                BNodeImpl bnode = (BNodeImpl) value;
                dateBNodeId = bnode.getID();
            }

            return true;
        }

        return false;
    }

    private boolean handleDateLabel(String subject, String predicate, String object) {
        if (subject.equals(dateBNodeId)) {
            // if dateBNodeId matches, we assume we're under a <collex:date> and simply
            // look for <rdfs:label> and <rdf:value>

            if ("http://www.w3.org/2000/01/rdf-schema#label".equals(predicate)) {
                addField(doc, "date_label", object);
                return true;
            }

            if ("http://www.w3.org/1999/02/22-rdf-syntax-ns#value".equals(predicate)) {
                //System.out.println( "handleDateLabel: " + object );
                ArrayList<String> years = parseYears(object);

                if( years.isEmpty() == true ) {
                    addError("Invalid date format: " + object);
                    return false;
                }

                // add the years
                for (String year : years) {
                    addFieldIfUnique(doc, "year", year);
                }

                // and any fields that are derived from the years
                addDerivedDateFields( years );

               return true;
            }
        }
        return false;
    }

    private boolean handleSource(String predicate, String object) {
        if ("http://purl.org/dc/elements/1.1/source".equals(predicate)) {
            addField(doc, "source", object);
            return true;
        }
        return false;
    }

    private boolean handleThumbnail(String predicate, String object) {
        if ("http://www.collex.org/schema#thumbnail".equals(predicate)) {
            addField(doc, "thumbnail", object);
            return true;
        }
        return false;
    }

    private boolean handleImage(String predicate, String object) {
        if ("http://www.collex.org/schema#image".equals(predicate)) {
            addField(doc, "image", object);
            return true;
        }
        return false;
    }

    private boolean handleURL(String predicate, String object) {
        if ("http://www.w3.org/2000/01/rdf-schema#seeAlso".equals(predicate)) {
            addField(doc, "url", object);
            return true;
        }
        return false;
    }

    private boolean handleHasPart(String predicate, String object) {
        if ("http://purl.org/dc/terms/hasPart".equals(predicate)) {
            addField(doc, "hasPart", object);
            return true;
        }
        return false;
    }

    private boolean handleIsPartOf(String predicate, String object) {
        if ("http://purl.org/dc/terms/isPartOf".equals(predicate)) {
            addField(doc, "isPartOf", object);
            return true;
        }
        return false;
    }

    private boolean handleText(String predicate, String object) {

        // first, check if this object is TEXT. If it is the predicate
        // will have the #text url below....
        if ("http://www.collex.org/schema#text".equals(predicate)) {

            String text = object;
            boolean externalText = false;
            if ( this.hasCorrectedText ) {
                // only in index mode do we attempt to grab 
                // corrected text from the full text folder
                if (config.mode == Mode.INDEX) {
                    externalText = true;
                    text = getCorrectedText();
                } else {
                    text = "";
                }
                
            } else {
                // Objects with external content will have some form of
                // http url as the content.
                if (object.trim().startsWith("http://") && object.trim().indexOf(" ") == -1) {
                    addFieldEntry(doc, "text_url", text, false);
    
                    // only in index mode do we attempt to grab 
                    // full text from the full text folder
                    if (config.mode == Mode.INDEX) {
                        externalText = true;
                        text = getFullText( text );
                    } else {
                        text = "";
                    }
                }
            }

            if ( text.length() > 0 || this.config.isPagesArchive() ) {
                this.largestTextField = Math.max(this.largestTextField, text.length());
                // NOTE: the !externalText signals to the add method that it
                // should NOT perform any cleanup. Text goes in untouched.
                addFieldEntry(doc, "text", text, false, !externalText);
            }

            return true;
        }
        return false;
    }
    
    /**
     * find the full path to the full text root baseed on 
     * the path to the original rdf sources
     * @return
     */
    private String findFullTextRoot() {
        String path = this.config.sourceDir.toString();
        int pos = path.indexOf("/rdf/");
        path = path.substring(0, pos) + "/fulltext/";
        path += RDFIndexerConfig.safeArchive(this.config.archiveName) + "/";
        return path;
    }
    
    /**
     * Get the corrected text for the current document
     * @return
     */
    private String getCorrectedText() {
        String fName = this.config.correctedTextMap.get(this.documentURI);
        File corrTxtFile = new File( this.config.correctedTextDir, fName);
        if (corrTxtFile.exists() == false) {
            this.errorReport.addError(new IndexerError("", this.documentURI, "Missing corrected text file " + corrTxtFile.toString()));
            return "";
        }
        FileInputStream is = null;
        try {
            is = new FileInputStream(corrTxtFile);
            return IOUtils.toString( is, "UTF-8");
        } catch (IOException e) {
            errorReport.addError(new IndexerError(corrTxtFile.toString(), this.documentURI, "Unable to read corrected text" + ": "
                + e.toString()));
            return "";
        } finally {
            IOUtils.closeQuietly(is);
        }
    }
    
    /**
     * Read the full text for <code>uri</code> from the fulltext area of the solr sources.
     * If any errors are encountered, log them and return an empty string
     * 
     * @param uri
     * @return A string containing the full text - or an empty string if errors occur.
     */
    private String getFullText(String uri) {

        String fullTextRoot = findFullTextRoot() ;
        File root = new File( fullTextRoot );
        if (root.exists() == false) {
            this.errorReport
                .addError(new IndexerError("", uri, "Missing full text source directory " + root.toString()));
            return "";
        }

        // convert URL into filename for text
        String name = uri.replaceAll("/", "SL");
        name = name.replace(":", "CL");
        name = name.replace("?", "QU");
        name = name.replace("=", "EQ");
        name = name.replace("&", "AMP");
        File textFile = new File(fullTextRoot + name + ".txt");
        if (textFile.exists() == false) {
            this.errorReport.addError(new IndexerError("", uri, "Missing full text file " + textFile.toString()));
            return "";
        }

        // read it!
        FileInputStream is = null;
        try {
            is = new FileInputStream(textFile);
            return IOUtils.toString( is, "UTF-8");
        } catch (IOException e) {
            errorReport.addError(new IndexerError(textFile.toString(), uri, "Unable to read full text" + ": "
                + e.toString()));
            return "";
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    private boolean handleRole(String predicate, String object) {
        if (predicate.startsWith("http://www.loc.gov/loc.terms/relators/")) {
            String role = predicate.substring("http://www.loc.gov/loc.terms/relators/".length());
            addField(doc, "role_" + role, object);
            addField(doc, "role", "role_" + role);
            return true;
        }
        return false;
    }

    public static ArrayList<String> parseYears(String value) {
        ArrayList<String> years = new ArrayList<String>();

        if ("unknown".equalsIgnoreCase(value.trim()) || uncertain.equalsIgnoreCase(value.trim())) {
            return( years );
        }

        // deal with embedded whitespace in ranges
        value = value.replace( ", ", "," ).replace( " ,", "," );

        StringTokenizer tokenizer = new StringTokenizer(value);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            int range = token.indexOf(',');
            int wild = token.indexOf('u');

            // if we have a leading alpha (e.g "Aug") it is ignored
            if( Character.isLetter( token.charAt( 0 ) ) == true ) {
                years.clear( );
                return( years );
            }

            // ranges containing wildcards are forbidden
            if( range != -1 && wild != -1 ) {
                years.clear( );
                return( years );
            }

            if( range != -1 ) {
                parseYearRange( years, token );
            } else if( wild != -1 ) {
                parseYearWild( years, token );
            } else {
                if( token.length() >= 4 ) {
                    years.add( token.substring( 0, 4 ) );
                } else {
                    // invalid date, less than 4 characters
                    years.clear( );
                    return( years );
                }
            }
        }
        return( years );
    }

    private void addDerivedDateFields( final ArrayList<String> years ) {

        // only process years that are in the correct format...
        Pattern p = Pattern.compile( "\\d{4}" );
        for( String year : years ) {
            Matcher m = p.matcher( year );
            if( m.matches( ) == true ) {
                //System.out.println( "YEAR [" + year + "] quarter [" + makeQuarterCentury( year ) + "] half [" + makeHalfCentury( year ) + "] full [" + makeCentury( year ) + "]" );
                addFieldIfUnique( doc, "decade", makeDecade( year ) );
                addFieldIfUnique( doc, "quarter_century", makeQuarterCentury( year ) );
                addFieldIfUnique( doc, "half_century", makeHalfCentury( year ) );
                addFieldIfUnique( doc, "century", makeCentury( year ) );
            }
        }
    }

    public static String makeDecade( final String year ) {
        return( year.substring( 0, 3 ) + "0" );
    }

    public static String makeQuarterCentury( final String year ) {
        Integer sub = Integer.parseInt( year.substring( 2, 4 ) );
        String quarter = "00";
        if( sub >= 75 ) quarter = "75";
        else if( sub >= 50 ) quarter = "50";
        else if( sub >= 25 ) quarter = "25";
        return( year.substring( 0, 2 ) + quarter );
    }

    public static String makeHalfCentury( final String year ) {
        Integer sub = Integer.parseInt( year.substring( 2, 4 ) );
        String half = ( sub >= 50 ) ? "50" : "00";
        return( year.substring( 0, 2 ) + half );
    }

    public static String makeCentury( final String year ) {
        return( year.substring( 0, 2 ) + "00" );
    }

    public void addField(HashMap<String, ArrayList<String>> map, String name, String value) {

        // skip null fields
        if (value == null || name == null)
            return;

        // if the field is a url, check to see if it is reachable
        if (config.collectLinks && value.trim().startsWith("http://") && value.trim().indexOf(" ") == -1
            && !"uri".equals(name)) {
            linkCollector.addLink(documentURI, this.file.toString(), value);
        }

        addFieldEntry(map, name, value, false);
    }

    public void addFieldIfUnique(HashMap<String, ArrayList<String>> map, String name, String value) {

        // skip null fields
        if (value == null || name == null)
            return;

        ArrayList<String> objectArray = map.get( name );
        if( objectArray == null || objectArray.contains( value ) == false ) {
            addFieldEntry(map, name, value, false);
        }
    }

    /**
     * Add a CLEANED field entry. The entry will be normalize, escape sequences stripped
     * and invalid utf-8 chars stripped
     * @param map
     * @param name
     * @param value
     * @param replace
     */
    private void addFieldEntry(HashMap<String, ArrayList<String>> map, String name, String value, Boolean replace) {
        addFieldEntry(map, name, value, replace, true);
    }

    /**
     * Add a new field entry and optionally clean the data
     * @param map
     * @param name
     * @param value
     * @param replace
     * @param clean
     */
    private void addFieldEntry(HashMap<String, ArrayList<String>> map, String name, String value, boolean replace, boolean clean) {

        // clean everything going in? 
        String data = value;
        if ( clean ) {
            data = TextUtils.stripEscapeSequences(data, this.errorReport, this.file, this.documentURI);
            data = TextUtils.normalizeWhitespace(data);
            data = TextUtils.stripUnknownUTF8(data, this.errorReport, this.file, this.documentURI);
        }
       
        // make sure we add to array for already existing fields
        if (map.containsKey(name) && replace == false) {
            ArrayList<String> pastValues = map.get(name);
            pastValues.add(data);
            map.put(name, pastValues);
        } else {
            ArrayList<String> values = new ArrayList<String>();
            values.add(data);
            map.put(name, values);
        }
    }


    private String getFirstField(HashMap<String, ArrayList<String>> object, String field) {
        ArrayList<String> objectArray = object.get(field);
        if (objectArray != null && objectArray.isEmpty( ) == false ) {
            return objectArray.get(0);
        }
        return "";
    }

    private String getLastField(HashMap<String, ArrayList<String>> object, String field) {
        ArrayList<String> objectArray = object.get(field);
        if (objectArray != null && objectArray.isEmpty( ) == false ) {
            return objectArray.get( objectArray.size( ) - 1 );
        }
        return "";
    }

    public HashMap<String, HashMap<String, ArrayList<String>>> getDocuments( boolean isPageData ) {
        if ( isPageData ) {
            return documents;
        }
        
        // add author_sort: we do that here because we have a few different fields we look at and the order they appear
        // shouldn't matter, so we wait to the end to find them.
        Set<String> keys = documents.keySet();
        for (String uri : keys) {
            HashMap<String, ArrayList<String>> object = documents.get(uri);
            String author = getFirstField(object, "role_AUT");
            String artist = getFirstField(object, "role_ART");
            String editor = getFirstField(object, "role_EDT");
            String publisher = getFirstField(object, "role_PUB");
            String translator = getFirstField(object, "role_TRN");
            String printer = getFirstField(object, "role_CRE");
            String etcher = getFirstField(object, "role_ETR");
            String engraver = getFirstField(object, "role_EGR");
            if (author.length() > 0)
                addField(object, "author_sort", author);
            else if (artist.length() > 0)
                addField(object, "author_sort", artist);
            else if (editor.length() > 0)
                addField(object, "author_sort", editor);
            else if (publisher.length() > 0)
                addField(object, "author_sort", publisher);
            else if (translator.length() > 0)
                addField(object, "author_sort", translator);
            else if (printer.length() > 0)
                addField(object, "author_sort", printer);
            else if (etcher.length() > 0)
                addField(object, "author_sort", etcher);
            else if (engraver.length() > 0)
                addField(object, "author_sort", engraver);

            // add year_sort fields
            String year_sort_min = getFirstField(object, "year");
            if (year_sort_min.isEmpty() == false ) {
                String year_sort_max = getLastField(object, "year");

                addField(object, "year_sort", year_sort_min);
                addField(object, "year_sort_asc", year_sort_min);
                addField(object, "year_sort_desc", year_sort_max);
            } else {
                addField( object, "year", uncertain );
                addField( object, "year_sort", uncertain );
                addField( object, "year_sort_asc", uncertain );
                addField( object, "year_sort_desc", uncertain );
            }

            // add fulltext and ocr indicators
            ArrayList<String> objectArray = object.get("text");
            if (objectArray != null) { // If we have a text field
                if (object.get("has_full_text") == null)
                    addField(object, "has_full_text", "T");
            } else {
                if (object.get("has_full_text") == null)
                    addField(object, "has_full_text", "F");
            }
            objectArray = object.get("is_ocr");
            if (objectArray == null) // If we weren't told differently, then it is not an ocr object
                addField(object, "is_ocr", "F");
            objectArray = object.get("freeculture");
            if (objectArray == null) // If we weren't told differently, then it is freeculture
                addField(object, "freeculture", "T");
        }
        return documents;
    }

    private static void parseYearWild( List<String> years, final String date ) {

        // expand 184u to 1840-1849
        char[] yearChars = date.toCharArray();
        int numLength = date.length();
        int i, factor = 1, startPos = 0;

        if (numLength > 4) numLength = 4;

        // increase factor according to size of number
        for (i = 0; i < numLength; i++) factor *= 10;

        // start looking for 'u', decreasing factor as we go
        for (i = startPos; i < numLength; i++) {
            if (yearChars[i] == 'u') {
                int padSize = numLength - i;
                String formatStr = "%0" + padSize + "d";
                // iterate over each year
                for (int j = 0; j < factor; j++) {
                    years.add(date.substring(0, i) + String.format(formatStr, j));
                }
                // once one 'u' char is found, we are done
                break;
            }
            factor = factor / 10;
        }
    }

    private static void parseYearRange( List<String> years, final String range ) {

        String [] tokens = range.split( "," );
        if( tokens.length != 2 ) {
            // more than 1 range delimiter
            years.clear( );
            return;
        }

        String start = tokens[ 0 ];
        String finish = tokens[ 1 ];
        if (start.length() >= 4 && finish.length() >= 4) {
            years.addAll( enumerateYears(start.substring(0, 4), finish.substring(0, 4)));
        } else {
            years.clear( );
            return;
        }
    }

    private static ArrayList<String> enumerateYears(String startYear, String endYear) {
        int y1 = Integer.parseInt(startYear);
        int y2 = Integer.parseInt(endYear);

        ArrayList<String> years = new ArrayList<String>();
        years.add(startYear);
        if (y2 <= y1)
            return years;

        for (int i = y1 + 1; i <= y2; i++) {
            years.add("" + i);
        }

        return years;
    }
    
    private void addError( final String message ) {
        this.errorReport.addError(new IndexerError(this.file.toString(), this.documentURI, message));
    }

    public void setFile(final File file) {
        this.file = file;
    }
    
    public long getLargestTextSize() {
        return this.largestTextField;
    }

    public void endRDF() throws RDFHandlerException {
        // no-op
    }

    public void handleComment(String arg0) throws RDFHandlerException {
        // no-op
    }

    public void handleNamespace(String arg0, String arg1) throws RDFHandlerException {
        // no-op
    }

    public void startRDF() throws RDFHandlerException {
        // no-op
    }
}
