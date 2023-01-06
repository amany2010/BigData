import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

public class WebCrawlerExampleWithDepth {
    // initialize MAX_DEPTH variable with final value
    static int i=2063;
    private static final int MAX_DEPTH = 2;
    // create set that will store links
    private HashSet<String> urlLinks;
    // initialize set using constructor
    public WebCrawlerExampleWithDepth() {
        urlLinks = new HashSet<>();
    }
    // create method that finds all the page link in the given URL
    public void getPageLinks(String URL, int depth) throws IOException {

        //we use the conditional statement to check whether we have already crawled the URL or not.
        // we also check whether the depth reaches to MAX_DEPTH or not
        if ((!urlLinks.contains(URL) && (i<10000))) {

            // use try catch block for recursive process
            try {
                // if the URL is not present in the set, we add it to the set
                urlLinks.add(URL);
                System.out.println("Level: "+depth+"    "+URL);
                // fetch the HTML code of the given URL by using the connect() and get() method and store the result in Document
                Document doc = Jsoup.connect(URL).get();
                String name="page"+i;i++;
                String path="/home/hduser/BigData/Pages/";
                FileWriter Page = new FileWriter(path+name);
                Page.write(URL+"\n");
                Page.write(Jsoup.parse(String.valueOf(doc.body())).text());
                BufferedReader br = new BufferedReader(new FileReader("/home/hduser/BigData/Pages/"+name));
                if (br.readLine() == null)i--;
                // we use the select() method to parse the HTML code for extracting links of other URLs and store them into Elements
                Elements availableLinksOnPage = doc.select("a[href]");
                // increase depth
                depth++;
                // for each extracted URL, we repeat above process
                for (Element page : availableLinksOnPage) {

                    // call getPageLinks() method and pass the extracted URL to it as an argument
                    getPageLinks(page.attr("abs:href"), depth);
                }
            }
            // handle exception
            catch (IOException e) {
                // print exception messages
                System.err.println("For '" + URL + "': " + e.getMessage());
            }
        }
    }
    // main() method start
    public static void main(String[]args) throws IOException {
        // create instance of the WebCrawlerExampleWithDepth class
        WebCrawlerExampleWithDepth obj = new WebCrawlerExampleWithDepth();

        // pick a URL from the frontier and call the getPageLinks()method and pass 0 as starting depth
        obj.getPageLinks("https://www.geeksforgeeks.org/fundamentals-of-algorithms/?ref=shm", 0);
    }
}
