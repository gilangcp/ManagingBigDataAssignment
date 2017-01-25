# Synopsis

This project contains programs for filtering, processing Twitter data in University of Twente Hadoop Cluster. All of the programs are written in Scala

It contains 3 main programs: 

- CountKeywordByUniqueUserPerMonth => Used for retrieving tweet count by keyword, by unique user per month. 
- CountUniqueUserPerMonth => Used for counting unique user per month
- LocationKeywordByUniqueUserPerMonth => Used for retrieving location by keyword, by unique user per month.


## Calling Parameter

- runTool CountKeywordByUniqueUserPerMonth [Keyword] [Source] [Destination]
- runTool CountUniqueUserPerMonth [Source] [Destination]
- runTool LocationKeywordByUniqueUserPerMonth [Keyword] [Source] [Destination]

## Output

- CountKeywordByUniqueUserPerMonth : (M-YYYY,count)
- CountUniqueUserPerMonth: (count,123456789)
- LocationKeywordByUniqueUserPerMonth: (Location,M-YYYY,count)


##Caveats
- For CountKeywordByUniqueUserPerMonth and LocationKeywordByUniqueUserPerMonth, the keyword parameter cannot handle space
- CountUniqueUserPerMonth cannot accept wildcard in source path, thus you need to manually specify each month folder 

## Contributors
- Gilang Charismadiptya (s1779524)
- Semere Kios (s1773933)
- Dimas Wibisono Prakoso (s1751425)
- Try Agustini (s1574728)
