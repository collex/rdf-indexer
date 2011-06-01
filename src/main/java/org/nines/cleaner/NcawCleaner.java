package org.nines.cleaner;

import org.nines.ICustomCleaner;

public class NcawCleaner implements ICustomCleaner {

    public String clean(String archiveName, String content) {
        
        if ( archiveName.equals("ncaw") == false) {
            return content;
        }
        
        // take file line by line. Keep text bound by 
        //    <td class="main_text"
        // or
        //    <td class="notes_text"
        // ending with </td>
        //
        String starters[] = {"<td class=\"main_text\"", "<td class=\"notes_text\""};
        String ender = "</td>";
        String[] lines = content.split("\n");
        StringBuffer finalContent = new StringBuffer();
        boolean skip = true;
        boolean lineHandled = false;
        for ( int i=0; i<lines.length; i++) {
            
            String line = lines[i].trim();
            lineHandled = false;
            
            // look for </td> when in midst of acceptinging multiline content
            if ( skip == false && line.contains(ender) ) {
                int p0 = line.indexOf(ender);
                line = line.substring(0,p0).trim();
                if ( line.length() > 0 ) {
                    finalContent.append(line).append("\n");
                }
                skip = !skip;
                lineHandled = true;
                continue;
            } 
            
            // look for any of the starters in this line...
            for ( int s=0; s<starters.length; s++) {
                String starter = starters[s];
                
                if ( line.contains(starter) ) {
                    int p0 = line.indexOf(starter);
                    int p1 = line.indexOf(">", p0);
                    line = line.substring(p1+1);
                    int p2 = line.indexOf(ender);
                    if ( p2 > -1) {
                        line = line.substring(0,p2).trim();
                        if ( line.length() > 0) {
                            finalContent.append(line).append("\n");
                        }
                    } else {
                        line = line.trim();
                        if (line.length() > 0) {
                            finalContent.append(line).append("\n");
                        }
                        skip = !skip;
                    }
                    lineHandled = true;
                    break;
                }
            }
     
            // if not handled yet, append text if we are not skipping
            if ( lineHandled == false ) {
                if ( skip == false ) {
                    finalContent.append(line).append("\n");
                }
            }
        }
        System.out.println(finalContent.toString());
        
        return finalContent.toString().trim();
    }
}
