# Give-me-for-the-number-of-rating  
#  Join the two tables and based upon the rating, give the stars

# Input data:  

![Input data](https://github.com/user-attachments/assets/b1828458-700e-4bf6-abea-07e5f2e205ee)  
#  Solution:  

data = [(1,"Veg Biryani"),(2,"Veg Fried Rice"),(3,"Kaju Fried Rice"),(4,"Chicken Biryani"),(5,"Chicken Dum Biryani"),(6,"Prawns Biryani"),(7,"Fish Birayani")]  
columns = ["food_id","food_item"]  
df1 = spark.createDataFrame(data, schema = columns)  
df1.show()  

ratings = [(1,5),(2,3),(3,4),(4,4),(5,5),(6,4),(7,4)]  
df2 = spark.createDataFrame(ratings,["food_id","rating"])  
df2.show()  

#Give the stars on the rating with seperate column  

joindf = df1.join(df2,["food_id"], "full")  
joindf.show()  

finaldf = joindf.withColumn("stars(out of 5)",expr("repeat('*',rating)"))  
finaldf.show()  

