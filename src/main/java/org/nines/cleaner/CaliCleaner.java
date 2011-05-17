package org.nines.cleaner;

import org.nines.ICustomCleaner;

public class CaliCleaner implements ICustomCleaner {

    public String clean(String archiveName, String content) {
        
        if ( archiveName.equals("cali")) {
            return stripJunk(content, "Search Text:", "fetching image...");
        }
        return content;
    }
    
    private String stripJunk(String content, String startWord, String stopWord) {
        String[] lines = content.split("\n");
        StringBuffer finalContent = new StringBuffer();
        boolean skip = true;
        for ( int i=0; i<lines.length; i++) {

            if ( lines[i].equals(startWord) || lines[i].equals(stopWord) ) {
                skip = !skip;
            } else {
                if ( skip == false ) {
                    finalContent.append(lines[i]).append("\n");
                }
            }
        }
        
        return finalContent.toString().trim();
    }
}
