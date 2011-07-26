package org.nines.cleaner;

import org.nines.ICustomCleaner;

public class NineteenCleaner implements ICustomCleaner {

    public String clean(String archiveName, String content) {
        
        if ( archiveName.equals("nineteen")) {
            String str = stripJunk(content, "<div xmlns=\"http://www.w3.org/1999/xhtml\">", "Back to context...");
            if (str.length() > 0)
            	return str;
            return stripJunk(content, "<p xmlns=\"http://www.w3.org/1999/xhtml\">", "<a class=\"action\"");
        }
        return content;
    }
    
    private String stripJunk(String content, String startWord, String stopWord) {
        String[] lines = content.split("\n");
        StringBuffer finalContent = new StringBuffer();
        boolean skip = true;
        for ( int i=0; i<lines.length; i++) {

            if ( lines[i].indexOf(startWord) > -1) {
            	skip = false;
            } else if (lines[i].indexOf(stopWord) > -1 ) {
            	break;
            } else {
                if ( skip == false ) {
                    finalContent.append(lines[i]).append("\n");
                }
            }
        }
        
        return finalContent.toString().trim();
    }

}
