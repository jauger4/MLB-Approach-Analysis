# MLB-Approach-Analysis
## Analysis of the MLB hitters and their approach
The goal of this project was to be able to rank MLB hitters from the 2025 season (as of 5/14/2025) by how focused their approach at the plate is towards power and contact. The long ball has always been a big part of the game of baseball. In today's game, more and more players are willing to swing and miss more often in order to swing harder and hit the ball farther. Because of this, I wanted to see which hitters have adopted this approach, as well as the players who are keeping the old-school contact approach. However, this does not mean that these players are the most effective at what they do. It is simply a measure of how often they keep their approach of a big swing or just putting the ball in play.

## How I came up with these numbers
The statistics that I used all came from [Baseball Savant](https://baseballsavant.mlb.com/). They have a wide variety of statistics to view and download. In order to get the results I was looking for, I chose to use the percentages at which each player:
- struck out (K%)
- walked (BB%)
- hit the ball hard (HH%)
- swung at pitches in the zone (IZS%)
- swung at pitches outside the zone (OZS%)
- made contact with pitches in the zone (IZC%)
- made contact with pitches outside the zone (OZC%)
- swung and missed (Whiff%)

### Formula for power
To calculate who was swinging for the fences the most, I used the formula:

> K% + BB% + HH% + Whiff% - OZS%

A true power hitter is going to end up swinging and missing more in order to swing harder, which is why I used K% and Whiff%. However, they are also going to be more selective with the pitches that they swing at, so they should be walking at a higher rate and swinging less at pitches outside of the zone. Lastly, you can't be a power hitter if you don't hit the ball hard. The formula adds together the percentages that should be higher, and it subtracts the percentage of outside of the zone swings, which should be lower. The output is a player's power rating, and the higher your rating, the more it seems you are swinging for the fences.

### Formula for contact
Now, to calculate who was simply trying to put the ball in play the most, I used the formula:

> (IZS% - OZS%) + (IZC% - OZC%) + BB% + K% + Whiff%

Contact hitters are guys that swing at anything near the zone and don't try to do too much with it. So, I took the rate at which they swung at putches in the zone and subtracted the rate at which they swung at pitches outside of the zone. This is to show how much they are swinging at everything. The narrower the margin between IZS% and OZS% shows that they will swing at anything. However, to ensure that they aren't just free-swingers, I also took the difference between IZC% and OZC% to ensure there was minimal fall-off between contact being made on the decision to swing outside of the zone. Because of the rate at which they swing, they are also going to walk less, so I added BB%. Lastly, the one thing contact guys absolutely cannot do is swing and miss or strikeout, which is why I added those in there as well. The formula is meant to find the player with the lowest sum of these percentages to show just how devoted they are to putting the ball in play.

## How I implemented the formula
In order to find who the top players in each category were, these were the tools I used:
- Python
- Apache Spark
- Microsoft Excel
- AWS S3 and EMR

To start, I had to clean the data from the CSV file. There wasn't much cleaning to do, but the data as downloaded had the column with the players' names as "last_name,first_name". This would cause an error in SQL, as it would recognize it as two different titles when it was meant to represent one.

From there, I wrote my Python scripts. I had one for each contact and power hitters. I used Spark libraries for these scripts, creating dataframes by calling the CSV file and adding SQL queries that called the player names and their ratings using the respective formula.

Now, in AWS, I can add my Python files as well as my CSV file into my S3 bucket. From there, I created a cluster in EMR for a Spark application. With this, I created steps for each Python file to be called and run. To call the data and store its output, I used these commands in the arguments section:

> --data_source s3://.../mlb_stats.csv
> 
> --output_uri s3://.../mlbPowerApproach

After doing that for both power and contact, I had folders in my S3 bucket titled "mlbPowerApproach" and "mlbContactApproach". Each had the results of my respective programs that I downloaded as CSV files, which I have uploaded to this repository.

## Future of this project
So far, I have just scratched the surface with this project. Net, I would like to find some data that can show the run production of players in the MLB. With that, I would like to compare power hitters and contact hitters to see who has been more productive. I also could potentially make a relative statistic that measures how much more of a power or contact approach a player has compared to the league average. An example of this would be how OPS+ shows a player's On Base Plus Slugging Percentage in relation to the rest of the league in that season, with 100 being the average. Baseball is a sport that never stops growing and changing, so I'm sure plenty of more ideas will continue to come to me as time goes on.
