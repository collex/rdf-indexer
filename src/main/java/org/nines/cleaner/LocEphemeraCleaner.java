package org.nines.cleaner;

import org.nines.ICustomCleaner;

public class LocEphemeraCleaner implements ICustomCleaner {

    public String clean(String archiveName, String content) {
        
        if ( archiveName.equals("locEphemera")) {
            return stripJunk(content, "<hr>", "Information about SGML version of this document.");
        }
        return content;
    }
    
    /**
     * Take all content after the SECOND <hr> tag up to the line about SGML.
     * Also skip all content found with { } .
     * @param content
     * @param startWord
     * @param stopWord
     * @return
     */
    private String stripJunk(String content, String startWord, String stopWord) {
        String[] lines = content.split("\n");
        StringBuffer finalContent = new StringBuffer();
        boolean skip = true;
        int startCnt = 0;
        boolean startDone = false;
        String line = "";
        boolean inBrace = false;
        for ( int i=0; i<lines.length; i++) {
            line = lines[i].trim();

            // once we have started accepting content we also need
            // to filter out content found between curly braces 
            // (and the braces themselves)
            if ( startDone == true ) {
                if ( line.contains("{") && line.contains("}") ) {
                    while ( true ) {
                        int p0 = line.indexOf("{");
                        if ( p0 == -1 ) {
                            break;
                        } else {
                            int p1 = line.indexOf("}");
                            if (p1 > -1 ) {
                                line = line.substring(0, p0) + line.substring(p1+1);
                            } else {
                                line = line.substring(0, p0);
                            }
                        }
                    }
                } else if ( line.contains("{")) {
                    inBrace = true;
                    finalContent.append(line).append( line.substring(0, line.indexOf("{")));
                    continue;
                } else if ( line.contains("}")) {
                    inBrace = false;
                    line = line.substring(line.indexOf("}"));
                }
                
                if ( inBrace ) {
                    continue;
                }
            }
            
            if ( line.toLowerCase().contains(startWord) && startDone == false ) {
                startCnt++;
                if (startCnt == 2) {
                    skip = !skip;
                    startDone = true;
                }
            } else if ( line.contains(stopWord) ) {
                skip = !skip;
            } else {
                if ( skip == false ) {
                    finalContent.append(line).append("\n");
                }
            }
        }
        
        return finalContent.toString().trim();
    }
}
