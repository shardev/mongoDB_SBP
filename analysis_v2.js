// -------------------------------------------------------------------------------------------------------------
// 2.1. What is most often used as a source of energy in the most energy-efficient buildings and which type of building is most often the most energy-efficient?
db.getCollection('rents').aggregate([
    {
        $match: {
            "energyEfficiencyClass": {$eq: "A"} 
            }
    },
    {
        $group: {
            "_id": { "typeOfFlat" : "$typeOfFlat" }, 
            "count": { $sum: 1},
            "typeOfHeating" : {$push : "$heatingType" }
        }
    },
    {
        $sort: {
            "count" : -1
            }
    },
    {
        $limit : 1
    },
    {
        $unwind :  "$typeOfHeating" 
    },
    {
        $group: {
            "_id": {
                "heating": "$typeOfHeating"
            },
            "countOfUnitsWithHeating": {
                $sum: 1
            },
            "tFlat" : { $first : "$_id.typeOfFlat" }
        }
  },
  {
      $sort : {
          "countOfUnitsWithHeating" : -1
      }
  },
  {
      $limit : 1
  }
])

// --------------------------------------------------------------------------------------
// 2.2 What is the average size of an apartment in m2 depending on the number of rooms in that apartment and the existence of a storage room in it?
// Display the total number of apartments for the given group, then the number of apartments that have certain amenities.
db.getCollection('rents').aggregate([
   {
       $group : { 
                "_id" : {
                    "noRooms" : "$noRooms"} ,
                avgArea : { $avg : "$livingSpace" },
                avgServiceCharge : {$avg : "$serviceCharge" },
                numberOfUnits : { $sum : 1},
                numberOfHasBalcony : {
                     $push : "$has"},
                numberOfHasKitchen : {
                     $sum: { $cond: [
                    { "$eq": [ "$has.hasKitchen", true ] }, 1, 0 ]}
                },
                numberOfHasLift : {
                     $sum: { $cond: [
                    { "$eq": [ "$has.lift", true ] }, 1, 0 ]}
                },
                numberOfHasGarden : {
                     $sum: { $cond: [
                    { "$eq": [ "$has.garden", true ] }, 1, 0 ]}
                }
        }
   },
   {
       $sort : { "numberOfUnits" : -1 }  
   } 
])

// --------------------------------------------------------------------------------------
// 2.3. From which year are there the most apartments and in which city and were they renovated in the last 10 years and what is the average rental price?
db.getCollection('rents').aggregate([
    {
        $match : { "lastRefurbish" : { $gt : 2011 } }
    },
    { 
        $group : { 
            "_id" : { "city" : "$regio2", "year" : "$yearConstructed" },
            avgRentsPerCity : { $avg : "$baseRent" },
            counterPerCity : { $sum : 1}
        }
    },
    { 
        $sort : { 
            "counterPerCity" : -1
        }
    },
    {
        $limit : 1
    }
])

// -----------------------------------------------------------------------------------------
// 2.4. In what year were the apartments built (if known) so that they are collectively the most expensive and how many apartments are there that allow pets in that year?
db.getCollection('rents').aggregate([
    {
        $match : {
            "yearConstructed" : {$ne : NaN}
        }
    },
    {
        $group : { 
            "_id" : "$yearConstructed", 
            "sumForYear" : { $sum : "$baseRent"},
            "petsAllowed": {
                $sum: { $cond: [
                    { "$eq": [ "$petsAllowed", "yes" ] },
                        1,
                        0 ]}
            } 
        }
    },
    {
        $project : { 
            "_id" : 0, 
            "yearConstr" : "$_id" , 
            "sumForYear" : 1, 
            "petsAllowed" : 1}
    },
    {
        $sort : { "sumForYear" : -1 }
    },
    {
        $limit : 1
    } 
])

// -----------------------------------------------------------------------------------------
// 2.5. Is the average price of apartments that have been renovated in the last 11 years higher than the average price of apartments for each of the cities?
db.getCollection('rents').aggregate([
    { 
        $group: {
            "_id": { 
                "cityPrimary": "$regio2",
            },
            "pricesRefurbish10y": { 
                 $sum: { 
                     $cond: [
                         { $gte: ['$lastRefurbish', 2010] } ,
                         "$baseRent", 
                          0 
                     ]
                }  
            },
            "counterRefurbish10y": { 
                 $sum: { 
                     $cond: [
                        { $gte: ['$lastRefurbish', 2010] },
                          1, 
                          0 
                     ]
                }  
            }
        }
     },
     {
        $lookup: {
                from: "cities",
                localField: "_id.cityPrimary",
                foreignField: "city",
                as: "cities_rents"
        }
     },
     {  
         $unwind :  "$cities_rents" 
     },
     {
         $project : {
                "_id" : 1, 
                "city" : 1,
                "avgPricesRefurbish10y" : {
                    $cond: [ { $eq: [ "$counterRefurbish10y", 0 ] }, 0, {$divide: [ "$pricesRefurbish10y", "$counterRefurbish10y" ] } ]     
                },
                "avgPricesPerCity": {
                    $divide: [ "$cities_rents.rentSum", "$cities_rents.unitCounter" ]   
                }
         }
     },
     {
         $project: {
            "_id" : 1, 
            "city" : 1,
            "avgPricesRefurbish10y" : 1,
            "avgPricesPerCity" : 1,
            "higherAvgRefurbishPrice" : { $cmp : ["$avgPricesRefurbish10y", "$avgPricesPerCity"]}
         }
     },
     {
         $sort : { "_id.city" : 1 }
     }
 ])
