package program;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoMain {

	private static MongoCollection<Document> collection;
	
	public static void main(String[] args) throws Exception {
		MongoClient c =  new MongoClient("localhost",27017);
		MongoDatabase db = c.getDatabase("test");
		collection = db.getCollection("movieDetails");
		String text = "Triar opci� : \n"+
				"1. Quines companyies tenen m�s de tres productes?\n"+
				"2. Hi ha companyies que tenen m�s d'un document on apareix el seu nom al camp name? \n"+
				"3 .Quines companyies tenen m�s productes? \n"+
				"4. Quines s�n les empreses que no tenen camp partners? \n"+
				"5. Quantes empreses hi ha?\n"+
				"6. Quines empreses hi ha que tinguin tres elements a l'array adquisitions?\n"+
				"7. Canvieu el nom del valor \"null\" del camp category pel valor \"buit\"? \n"+
				"8. Afegiu un camp nou que es digui \"present\", i per totes aquelles empreses de dates de creaci� anterior a l'any 2008, el valor del camp ha de ser \"no\".\n"+
				"9. Actualitzar la collection afegint la competici� \"Minnie\" amb permalink \"minnie\" a les empresesque tenen el competitor name \"Wikia\".\n"+
				"10. Quines companyies de les que s�n de category_codi \"advertising\", tenen m�s de tres competitions? \n";
		System.out.println(text);

		try{
			String s = "";
			while("exit".compareTo(s)!=0){
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				s = bufferRead.readLine();
				executarMetode(s);
			}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}

	private static void executarMetode(String opcio){
		switch (opcio) {
		//DONE
		case "1":
			mesde3Productes();
			break;
		//ALMOST DONE
		case "2":
			campName();
			break;
		//DONE
		case "3":
			mesProductes();
			break;
		//DONE
		case "4":
			companyiesSenseCampsPartners();
			break;
		//DONE
		case "5":
			quantesEmpresesHiha();
			break;
		//DONE
		case "6":
			tresElementsArrayAdquisitions();
			break;
		//DONE
		case "7":
			update();
			break;
		case "8":
			afegirCamp();
			break;
		case "9":
			updateMinnie();
			break;
		//DONE
		case "10":
			categoryCode();
			break;
		default:
			break;
		}
	}


	private static void categoryCode() {
	 
		Document value1 = new Document("$size", 3);		
		Document query = new Document("category_code","advertising");
		query.append("competitions", value1);
		
		FindIterable<Document> result = collection.find(query);
		long numbers = 0;
		for (Document doc : result) {
			numbers+=1;
			//System.out.println(doc.toJson());
		} System.out.println(numbers);
				
	}

	private static void updateMinnie() {
		
		Document searchFilter = new Document("name", "Wikia");
		Document key = new Document("competitors", searchFilter);
		FindIterable<Document> found = collection.find(searchFilter);
		
		if (found != null) {
			System.out.println("HOLA");
			
			Bson updatedValue = new Document("name","Minnie");
			
		}
		
		
	}

	private static void afegirCamp() {
		
	}

	private static void update() {

		Document search = new Document("category_code", null);
		FindIterable<Document> found = collection.find(search);
		
		if (found != null) {			
			
			Bson updatedValue = new Document("category_code", "buit");
			Bson updateOperation = new Document("$set", updatedValue);		
			
			//Use Safran to check results
			for (Document doc : found) {							
				collection.updateMany(doc, updateOperation);
			}
			
		}
	}

	private static void tresElementsArrayAdquisitions() {
			
		Document search = new Document("$size", 3);
		Document query = new Document("acquisitions", search);
		FindIterable<Document> cursor = collection.find(query);	
		long numbers = 0;
		for(Document doc: cursor){
			numbers+=1;
 		 	//System.out.println(doc.toJson());
      }	System.out.println(numbers);
		
	}

	private static void quantesEmpresesHiha() {
		
		Document search = new Document("$exists", true);
		Document query = new Document("name", search);
		long numbers = collection.count(query);
		System.out.println(numbers);
		
		
	}

	private static void companyiesSenseCampsPartners() {
		
		Document search = new Document("$size", 0);
		Document query = new Document("partners",search);
		
		long numbers = 0;
		FindIterable<Document> cursor = collection.find(query);		
		for(Document doc: cursor){
			numbers+=1;
 		 	//System.out.println(doc.toJson());
      }	
		System.out.println(numbers);
		
	}

	private static void mesProductes() {
		
		Document grupD2 = new Document("_id","null").append("maxim",new Document("$max","$products"));
		Document group2 = new Document("$group",grupD2);
		
		ArrayList al = new ArrayList();
		al.add(group2);
		AggregateIterable out = collection.aggregate(al);
		MongoCursor cursor = out.iterator();
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}		
	}

	private static void campName() {

		Block<Document> printBlock= document -> System.out.println(document.toJson());
		/*
		AggregateIterable<Document> output = collection.aggregate(
				
				Arrays.asList(	new Document("$name", new Document())
								
						)				
				);
		
		*/		
		
		collection.aggregate(
				Arrays.asList( Aggregates.match(Filters.eq("name","iML")),
							   Aggregates.group("$name", Accumulators.sum("count",1))
								
						)
				).forEach(printBlock);
		
	}

	private static void mesde3Productes() {

		FindIterable<Document> cursor = collection.find(where("this.products.length > 3"));
	      for(Document doc: cursor){
	 		 	System.out.println(doc.toJson());
	      }		
	}
}
