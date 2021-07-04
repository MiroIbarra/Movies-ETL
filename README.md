# Background
Assist Britta with creating an automated pipeline that will take in new data, perform the appropriate transformations, and load data into existing tables. 

## ETL Function to Read Three Data Files

 1. Add dependencies (import json, pandas, numpy, regular expressions, psycopg2, time and from sqlalchemy import create_engine). Create a function that takes in three arguments; Wikipedia data, Kaggle metadata, and MovieLens rating data (from Kaggle)
 2. Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames.  
 3. Open the read the Wikipedia data JSON file.  
 4. Read in the raw wiki movie data as a Pandas DataFrame. 
 5. Return the three DataFrames
 6. Create the path to your file directory and variables for the three files. 
 7. Set the three variables in Step 6 equal to the function created in Step 1.
	
		Example: wiki_file, kaggle_file, ratings_file = extract_transform_load()
 8. Set the DataFrames from the return statement equal to the file names in Step 6.
 9. Confirm that the Dataframe loaded correctly by printing wiki movies Dataframe and kaggle metadata Dataframe top rows. 

##   Extract and Transform the Wikipedia Data 
### Using Python, Pandas, the ETL process, and code refactoring, extract and transform the Wikipedia data to merge it with the Kaggle metadata. While extracting the IMDb IDs using a regular expression string and dropping duplicates, use a try-except block to catch errors.
1.  Add dependencies (import json, pandas, numpy, regular expressions, psycopg2, time and from sqlalchemy import create_engine). 
2. Add the clean movie function that takes in the argument, "movie". Merge columns.
3. Create a function that takes in three arguments; Wikipedia data, Kaggle metadata, and MovieLens rating data (from Kaggle). Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames. Open the read the Wikipedia data JSON file.  
4. Write a list comprehension to filter out TV shows.
5. Write a list comprehension to iterate through the cleaned wiki movies list and call the clean_movie function on each movie.
6. Read in the cleaned movies list from Step 4 as a DataFrame.
7. Write a try-except block to catch errors while extracting the IMDb ID using a regular expression string and dropping any imdb_id duplicates. If there is an error, capture and print the exception.
   
   Example:
   
   try:
	   wiki_movies_df['imdb_id'] = 	wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
	   wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
        
    except:
        print("No imdb link")

8. Write a list comprehension to keep the columns that don't have null values from the wiki_movies_df DataFrame.
   
    Example:
    	
    	[column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]

9. Create a variable that will hold the non-null values from the “Box office” column.
10. Convert the box office data created in Step 8 to string values using the lambda and join functions.
11. Write a regular expression to match the six elements of "form_one" of the box office data.
    	
 		Example:	
    	
    	form_one = r'\$\d+\.?\d*\s*[mb]illion'
    	
12. Write a regular expression to match the three elements of "form_two" of the box office data.
 
 Example:

	form_two = r'\$\d{1,3}(?:,\d{3})+'
	
13. Add the parse_dollars function.
14. Clean the box office column in the    
15. Clean the budget column in the "wiki_movies_df" DataFrame.
16. Clean the release date column in the wiki_movies_df DataFrame.
17. Clean the running time column in the wiki_movies_df DataFrame.
18. Return three variables. The first is the wiki_movies_df DataFrame
19. Create the path to your file directory and variables for the three files.
20. Set the three variables equal to the function created in D1.
21. Set the wiki_movies_df equal to the wiki_file variable. 
22. Check that wiki_movies_df DataFrame columns are correct. 

## Extract and Transform the Kaggle Data
### Using Python, Pandas, the ETL process, and code refactoring, extract and transform the Kaggle metadata and MovieLens rating data, then convert the transformed data into separate DataFrames. Then, merge the Kaggle metadata DataFrame with the Wikipedia movies DataFrame to create the movies_df DataFrame. Finally, merge the MovieLens rating data DataFrame with the movies_df DataFrame to create the movies_with_ratings_df.
1. Add the function from the first deliverable, 
2. Use the same code as the second deliverable. 
3. Clean Kaggle metadata. 
4. Merge the two DataFrames into the movies DataFrame.
5. Drop unnecessary columns from the merged DataFrame.
6. Add in the function to fill in the missing Kaggle data.
7.  Call the function in Step 5 with the DataFrame and columns as the arguments.
8. Filter the movies DataFrame for specific columns.
9. Rename the columns in the movies DataFrame.
10. Transform and merge the ratings DataFrame.
11. Create the path to your file directory and variables for the three files.
12. Set the three variables equal to the ETL function created in D1.   
13. Set the DataFrames from the return statement equal to the file names in Step 12. 
14. Check the wiki_movies_df and movies_with_ratings_df Dataframe by printing the first five rows. 

## Create the Movie Database
### Use Python, Pandas, the ETL process, code refactoring, and PostgreSQL to add the movies_df DataFrame and MovieLens rating CSV data to a SQL database.
1. Make a copy of the previous code. and uncomment "from config import db_password" in the first cell.
2. Remove "return wiki_movies_df, movies_with_ratings_df, movies_df"
3. After the movies_with_ratings_df transform and merge the ratings DataFrameby adding the code to create the connection to the PostgreSQL database, then add the movies_df DataFrame to a SQL database.
    
    	Example:
    	db_string = f"postgresql://postgres:{db_password}@127.0.0.1:5432/movie_data" engine = create_engine(db_string)
    
    	movies_df.to_sql(name='movies', if_exists='replace', con=engine)
    	rows_imported = 0
    	# get the start_time from time.time()
    
    	start_time = time.time()
    	for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='append')
        rows_imported += len(data)

        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')
4. Run the program and open PostgreSQL database.
5. Run a query to count the number of rows for the movies and ratings tables. 
