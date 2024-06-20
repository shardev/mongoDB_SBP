// 1.1 How much does the existence of an elevator in a building affect the price of certain types of apartments?
// Grouping of apartments by the existence of an elevator and by type of apartment and calculating the average price for apartments grouped in this way
db.getCollection('rents').aggregate([
    {
        $match : {
            "typeOfFlat" : {$ne : NaN},
            "baseRent" : {$ne : NaN}
        }
    },
    {
        $group : {
            "_id" : {"type" : "$typeOfFlat", "lift" : "$lift"},
            "numOfUnits" : {$sum : 1},
            "sumBaseRent" : {$sum : "$baseRent"}
        }
    },
    {
        $project : {
            "avgBaseRent" : {$divide : ["$sumBaseRent", "$numOfUnits"]}
        }
    },
    {
        $sort : {
            "avgBaseRent" : -1
        }
    }
    
])



db.getCollection('rents').aggregate([
    {
        $match : {
            "typeOfFlat" : {$ne : NaN},
            "baseRent" : {$ne : NaN}
        }
    },
    {
        $group : {
            "_id" : {"type" : "$typeOfFlat", "lift" : "$has.lift"},
            "numOfUnits" : {$sum : 1},
            "sumBaseRent" : {$sum : "$baseRent"}
        }
    },
    {
        $project : {
            "avgBaseRent" : {$divide : ["$sumBaseRent", "$numOfUnits"]}
        }
    },
    {
        $sort : {
            "avgBaseRent" : -1
        }
    }
    
])


// 1.2/2.2 What is the most expensive/cheapest rent per square meter for each of the cities?
db.getCollection('rents').aggregate([
    {
        $match : {
            "livingSpace" : { $gte : 20 },
            "baseRent" : {$lte : 4000}
        }
    },
    {
        $group : {
            "_id" : {"city" : "$regio3"},
            "maxAvgMeterPrice" : {$max : {$divide : ["$baseRent", "$livingSpace"]}},
            "minAvgMeterPrice" : {$min : {$divide : ["$baseRent", "$livingSpace"]}}
        }
    },
    {
        $sort : {
            "maxAvgMeterPrice" : -1
        }
    }
])

// 1.3/2.3 For renovated apartments, determine the percentage of apartments that allow/do not allow/or there is a possibility
// moving in pets grouped by the quality of the interior of the apartment.
db.getCollection('rents').aggregate([
    {
        $match : {
            "lastRefurbish" : {$ne : NaN},
            "interiorQual" : {$ne : NaN},
            "petsAllowed" : {$ne : NaN}
        }
    },
    {
        $group : {
            "_id" : {"interior" : "$interiorQual"},
            "numberOfUnits" : {$sum : 1},
            "petsYes" : {$sum : {$cond : [{$eq : ["$petsAllowed", "yes"]}, 1, 0]}},
            "petsNo" : {$sum : {$cond : [{$eq : ["$petsAllowed", "no"]}, 1, 0]}},
            "petsNeg" : {$sum : {$cond : [{$eq : ["$petsAllowed", "negotiable"]}, 1, 0]}}
        }
    },
    {
        $project: {
            "percentOfAllowedPets" : {$divide : ["$petsYes", "$numberOfUnits"]},
            "percentOfNotAllowedPets" : {$divide : ["$petsNo", "$numberOfUnits"]},
            "percentOfNegAllowedPets" : {$divide : ["$petsNeg", "$numberOfUnits"]}
        }
    },
    {
        $sort: {
            "percentOfNotAllowedPets" : -1
        }
    }
])

//1.4/2.4 In what period were the apartments with the highest/lowest average rental price built?
db.getCollection('rents').aggregate([
    {
        $match : {
            $and : [ {"totalRent" : {$ne : NaN}}, {"totalRent" : {$lte : 5000}}]
        }
    },
    {
        $group : {
            "_id" : null,
            "sumTotalRentBef1900" : {$sum : {$cond : [{$lt : ["$yearConstructed",1900]}, "$totalRent", 0]}},
            "cntTotalRentBef1900" : {$sum : {$cond : [{$lt : ["$yearConstructed",1900]}, 1, 0]}},
            "sumTotalRent1900-1950" : {$sum : {$cond : [{$and : [ {$gte : ["$yearConstructed",1900]}, {$lt : ["$yearConstructed",1950]}]}, "$totalRent", 0]}},
            "cntTotalRent1900-1950" : {$sum : {$cond : [{$and : [ {$gte : ["$yearConstructed",1900]}, {$lt : ["$yearConstructed",1950]}]}, 1, 0]}},
            "sumTotalRent1950-2000" : {$sum : {$cond : [{$and : [ {$gte : ["$yearConstructed",1950]}, {$lt : ["$yearConstructed",2000]}]}, "$totalRent", 0]}},
            "cntTotalRent1950-2000" : {$sum : {$cond : [{$and : [ {$gte : ["$yearConstructed",1950]}, {$lt : ["$yearConstructed",2000]}]}, 1, 0]}},
            "sumTotalRentAft2000" : {$sum : {$cond : [{$gte : ["$yearConstructed",2000]}, "$totalRent", 0]}},
            "cntTotalRentAft2000" : {$sum : {$cond : [{$gte : ["$yearConstructed",2000]}, 1, 0]}}
        }
    },
    {
        $project : {
            "avgRentBef1900" : {$divide : ["$sumTotalRentBef1900","$cntTotalRentBef1900"]},
            "avgRent1900-1950" : {$divide : ["$sumTotalRent1900-1950","$cntTotalRent1900-1950"]},
            "avgRent1950-2000" : {$divide : ["$sumTotalRent1950-2000","$cntTotalRent1950-2000"]},
            "avgRentAft2000" : {$divide : ["$sumTotalRentAft2000","$cntTotalRentAft2000"]}
        }
    }
   
])

//1.5/2.5 How many apartments, grouped by room type, are such that they have a washing machine and a bathtub in the bathroom?
db.getCollection('rents').aggregate([
    {
        $match : {
            "facilities" : {$ne : NaN},
            "noRooms" : {$lte : 8},
            $and : [{"facilities" : { $regex: /[Bade]?wanne/}},{"facilities" : { $regex: /[W w]aschmaschine/ }}]
        }
    },
    {
        $group: {
            "_id" : {"noOfRooms" : "$noRooms"},
            "numOfUnits" : {$sum : 1}
        }
    },
    {
        $project : {
            "numOfFlatsWithLaundryAndBath" : "$numOfUnits"
        }
    },
    {
        $sort : {
            "numOfFlatsWithLaundryAndBath" : -1
        }
    }
])


1.5 Za rad sa indeksom

db.getCollection('rents').aggregate([
    {
        $match : {
            "facilities" : {$ne : NaN},
            "noRooms" : {$lte : 8},
            $text : {$search : "wanne"},
            $text : {$search : "Waschmaschine"}
        }
    },
    {
        $group: {
            "_id" : {"noOfRooms" : "$noRooms"},
            "numOfUnits" : {$sum : 1}
        }
    },
    {
        $project : {
            "numOfFlatsWithLaundryAndBath" : "$numOfUnits"
        }
    },
    {
        $sort : {
            "numOfFlatsWithLaundryAndBath" : -1
        }
    }
])
