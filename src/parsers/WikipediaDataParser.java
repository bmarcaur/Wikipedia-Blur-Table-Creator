package parsers;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;

public class WikipediaDataParser {
	
	private static String fileLocation = System.getProperty("user.home") + "/Downloads/wikichallenge_data_all/";
	private static TreeMap<String, ArrayList<ArrayList<String>>> revisionData = new TreeMap<String, ArrayList<ArrayList<String>>>();
	private static TreeMap<String, String> commentData = new TreeMap<String, String>();
	private static TreeMap<String, String[]> articleInformation = new TreeMap<String, String[]>();
	
	public static TreeMap<String, ArrayList<ArrayList<String>>> parseRevisions() {
		try {
			// Call the comment method and add the all the revision comments
			parseComments();
			// Open revision file
			BufferedReader revisionReader = new BufferedReader(new FileReader(fileLocation + "training.tsv"));
			// Discard the Schema Line
			revisionReader.readLine();
			// Buffer for the line in the file
			String returnedLine;
			while((returnedLine = revisionReader.readLine()) != null){
				// Grab the first "column" which is the user id
				ArrayList<String> parsedRevisionInfo = new ArrayList<String>(13);
				parsedRevisionInfo.addAll(Arrays.asList(returnedLine.split("\\t")));
				// Grab the comment from the comment data structure
				parsedRevisionInfo.add(commentData.get(parsedRevisionInfo.get(2)));
				// Free up memory after the comment was used
				commentData.remove(parsedRevisionInfo.get(2));
				
				// Grab the user id for the key
				String userId = parsedRevisionInfo.get(0);
				// Add the revision object to the map
				if (revisionData.containsKey(userId)){
					revisionData.get(userId).add(parsedRevisionInfo);
				} else {
					ArrayList<ArrayList<String>> newRevisionList = new ArrayList<ArrayList<String>>(10);
					newRevisionList.add(parsedRevisionInfo);
					revisionData.put(userId, newRevisionList);
				}
			}
			revisionReader.close();
		} catch (FileNotFoundException e) {
			System.err.print("File was not found");
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e){
			System.err.print("A problem occurred while trying to read from the buffered stream.");
			e.printStackTrace();
			System.exit(1);
		}
		
		// Return a reference to the map we just created
		return revisionData;
	}
	
	public static TreeMap<String, ArrayList<String>> parseArticleInformation() {
		return null;
	}

	private static void parseComments() {
		try {
			BufferedReader commentReader = new BufferedReader(new FileReader(fileLocation + "comments.tsv"));
			// Discard the Schema Line
			commentReader.readLine();
			// Buffer for the line in the file
			int countedLines = 0;
			String returnedLine;
			while((returnedLine = commentReader.readLine()) != null && countedLines < ){
				String[] parsedCommentInfo = returnedLine.split("\\t");
				commentData.put(parsedCommentInfo[0], parsedCommentInfo[1]);	
				System.out.println(i++);
			}
			commentReader.close();
		} catch (FileNotFoundException e) {
			System.err.print("File was not found");
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e){
			System.err.print("A problem occurred while trying to read from the buffered stream.");
			e.printStackTrace();
			System.exit(1);
		}
	}
}
