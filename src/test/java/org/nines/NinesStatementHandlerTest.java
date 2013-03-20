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
import java.util.ArrayList;
import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import org.junit.Test;

public class NinesStatementHandlerTest extends TestCase {
    private NinesStatementHandler sh;
    private ErrorReport errorReport;

    protected void setUp() throws Exception {
        super.setUp();
        errorReport = new ErrorReport(new File("test_data", "test_report.txt"));
        sh = new NinesStatementHandler(errorReport, new LinkCollector(), new RDFIndexerConfig());
    }

    @Test
    public void testPdfStrip() {
        try {
            FileInputStream is = new FileInputStream(new File("test_data/sample.pdf"));
            PDDocument pdfDoc = PDDocument.load(is);
            assertEquals(2, pdfDoc.getNumberOfPages());
            PDFTextStripper pdfStrip = new PDFTextStripper();
            String text = pdfStrip.getText(pdfDoc);

            assertNotNull(text);
            System.out.println(text);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void testAddField() {
        HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();

        sh.addField(map, "Genre", "Poetry");
        assertTrue(map.containsKey("Genre"));
        ArrayList<String> values = map.get("Genre");
        assertTrue(values.size() == 1);

        sh.addField(map, "Genre", "Primary");
        assertTrue(map.containsKey("Genre"));
        values = map.get("Genre");
        assertTrue(values.size() == 2);
    }

    public void testYearParsing() {
        ArrayList<String> years = NinesStatementHandler.parseYears("184u");
        assertEquals(10, years.size());
        assertEquals("1840", years.get(0));
        assertEquals("1849", years.get(9));

        years = NinesStatementHandler.parseYears("1862-12-25,1864-01-01 1875 1954-10");
        assertEquals(5, years.size());
        assertEquals("1862", years.get(0));
        assertEquals("1954", years.get(4));

        years = NinesStatementHandler.parseYears("  Uncertain  ");
        assertEquals(1, years.size());
        assertEquals("Uncertain", years.get(0));
    }

    public void testUnknownYears() {
        ArrayList<String> years = NinesStatementHandler.parseYears("unknown");
        assertEquals(1, years.size());
        assertEquals("Uncertain", years.get(0));
    }
}
