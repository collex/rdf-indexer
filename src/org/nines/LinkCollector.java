package org.nines;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

public class LinkCollector
{
    private Writer dataWriter;

    /**
     * Testing LinkCollector that writes to System.out instead
     * of a file
     */
    public LinkCollector()
    {
        dataWriter = new PrintWriter(System.out, true);
    }
    
    /**
     * Standard LinkCollector that writes data out to a link data
     * file with the prefix specified. If the specified prefix does not
     * exist, data will be streamed to System.out
     * @param prefix
     * @throws IOException Throws of the prefix does not exist
     */
    public LinkCollector(String prefix)
    {
        try
        {
            dataWriter = new FileWriter(prefix + "_link_data.txt", true);
        }
        catch (IOException e)
        {
            dataWriter = new PrintWriter(System.out, true);
        }
    }

    public void addLink(String documentURI, String filename, String url)
    {
        try
        {
            dataWriter.write(documentURI + "\t" + filename + "\t" + url + "\n");
            dataWriter.flush();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void close()
    {
        try
        {
            dataWriter.close();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
