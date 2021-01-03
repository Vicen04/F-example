open System
open System.Diagnostics
open System.IO
open System.Collections.Concurrent
open System.Threading.Tasks
open System.Collections.Generic
open System.Threading
open System.Linq.Expressions
open System.Threading.Tasks

// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

//to get the shop code from the shop name
module ShopNames =
   let ShopCodes: Dictionary<String, String> = Dictionary()

//hardcoded paths
module StoreFilesAndData =
    let storeDataFolder = "StoreData" 
    let storeCodesFile = "StoreCodes.csv"


 //strings to populate
module ConsoleReadOptions =
    let mutable week = String.Empty
    let mutable year = String.Empty
    let mutable provider = String.Empty
    let mutable productType = String.Empty
    let mutable shopcode = String.Empty
    let mutable shopname = String.Empty


// conditions for the calculation
module Conditions =
    let mutable allProviders = true
    let mutable allProducts = true
    let mutable allDates = true
    let mutable allStores = true

module selections =
    let mutable finished = String.Empty
    let mutable validOption = true

let ReadFiles (data: string) =
   
   let orderSplit = data.Split(',')
   let cost = 
       if (Conditions.allProviders && Conditions.allProducts) then Convert.ToDouble(orderSplit.[2])
       elif Conditions.allProviders <> true && Conditions.allProducts then 
         if ConsoleReadOptions.provider <> orderSplit.[0] then   0.0               
         else  Convert.ToDouble(orderSplit.[2])
       elif (Conditions.allProviders && Conditions.allProducts <> true) then
         if ConsoleReadOptions.productType <> orderSplit.[1] then  0.0
         else Convert.ToDouble(orderSplit.[2])        
       else  
        if (ConsoleReadOptions.provider = orderSplit.[0] && ConsoleReadOptions.productType = orderSplit.[1]) then Convert.ToDouble(orderSplit.[2])       
        else  0.0   
        
   cost

let GetFiles (filenames : string)=        
    let fileNameExt = Path.GetFileName(filenames)
    let filename = Path.GetFileNameWithoutExtension(filenames)
    let filenamesplit = filename.Split('_')
    let orderData =
       if (Conditions.allDates && Conditions.allStores) then 
           File.ReadAllLines(StoreFilesAndData.storeDataFolder + @"\" + fileNameExt)    
       elif (Conditions.allDates && Conditions.allStores <> true) then
           if (filenamesplit.[0] = ConsoleReadOptions.shopcode) then  File.ReadAllLines(StoreFilesAndData.storeDataFolder + @"\" + fileNameExt) 
           else null
       elif (Conditions.allDates <> true && Conditions.allStores) then 
           if (filenamesplit.[1] = ConsoleReadOptions.week && filenamesplit.[2] = ConsoleReadOptions.year) then  File.ReadAllLines(StoreFilesAndData.storeDataFolder + @"\" + fileNameExt) 
           else null
       else 
           if (filenamesplit.[0] = ConsoleReadOptions.shopcode && filenamesplit.[1] = ConsoleReadOptions.week && filenamesplit.[2] = ConsoleReadOptions.year) then  File.ReadAllLines(StoreFilesAndData.storeDataFolder + @"\" + fileNameExt) 
           else null

    if orderData <> null then  
      let values = new ConcurrentQueue<double>()
      Parallel.For(0, 2, fun loop ->               
                     for i = loop * orderData.Length / 2 to  ((loop + 1) * orderData.Length / 2) do   
                        if i <> orderData.Length then
                           values.Enqueue(ReadFiles(orderData.[i]))  
                     
                   )|>ignore     
      let cost = Array.sum(values.ToArray());
      cost
    else
      let cost = 0.0
      cost    

let Calculate() =
    printfn "Please, wait until the calculation finishes"
    printfn ""
    let timer = new System.Diagnostics.Stopwatch()
    timer.Start()          
    let filenames = Directory.GetFiles(StoreFilesAndData.storeDataFolder) 
    let values = new ConcurrentQueue<double>()
    
    let value1 = ref 0
    let value2 = ref 0
    ThreadPool.GetAvailableThreads( value1,  value2)
    let workerThreads = value1.Value 
    let threads = (workerThreads / 4)
    let filesToread = filenames.Length / threads
    let extraFiles = filenames.Length % threads
    let addedThreads = extraFiles / filesToread
    let filesForTask = extraFiles % filesToread;
            
    Parallel.For(0, threads + addedThreads, fun loop ->                    
                  for j = loop * filesToread to (filesToread * (loop + 1)) - 1 do
                     if j < filenames.Length then
                       values.Enqueue(GetFiles(filenames.[j]))  
                  )|>ignore
                                   
    if filesForTask <> 0 then            
      let extraTask = Task.Factory.StartNew(fun () ->                
         for  i = 0 to  filesForTask do
             values.Enqueue(GetFiles(filenames.[filenames.Length - extraFiles - 1 + i]))
             )
      extraTask.Wait()          
                  
    let totalCost = Array.sum(values.ToArray())        
    timer.Stop()
    printfn "The total cost for this selection is %A" totalCost
    printfn "Time to calculate: %A" timer.Elapsed.TotalSeconds

let GetShop() = 
    Conditions.allStores <- false
    let mutable notvalidShop = true  
    while notvalidShop <> false do
       notvalidShop <- false 
       printfn "Please, select how to enter a shop:"
       printfn "1. With the shop code (Be aware that if you type it wrong, it's not going to find anything)"
       printfn "2. With the shop name" 
       printfn ""
       let selection = Console.ReadLine()
       printfn ""
       if selection = "1" then
           printfn "Please,  enter the shop code:"
           printfn ""
           ConsoleReadOptions.shopcode <- Console.ReadLine()           
       elif selection = "2" then
           printfn "Please,  enter the shop name:"
           printfn ""
           ConsoleReadOptions.shopname <- Console.ReadLine()
           let shopcode = ref ""
           if ShopNames.ShopCodes.TryGetValue(ConsoleReadOptions.shopname, shopcode) then
              ConsoleReadOptions.shopcode <- shopcode.Value              
           else
              printfn ""
              printfn "Shop not found, try it again"
              notvalidShop <- true
       else
           printfn "Please, select a valid option"            
           notvalidShop <- true
       printfn ""

let GetDate() = 
    Conditions.allDates <- false
    printfn "Please, type a number between 1 to 52 for the week (Be aware that if you type a different number, it's not going to find anything)"
    ConsoleReadOptions.week <- Console.ReadLine()
    printfn ""
    printfn "Please, type 2013 or 2014 for the year (Be aware that if you type a different number, it's not going to find anything)"
    ConsoleReadOptions.year <- Console.ReadLine()
    printfn ""

let GetProvider() = 
    Conditions.allProviders <- false
    printfn "Please, enter the producer (Be aware that if you type it wrong, it's not going to find anything)"
    ConsoleReadOptions.provider <- Console.ReadLine()
    printfn ""

let GetProduct() = 
    Conditions.allProducts <- false
    printfn "Please, enter the product type (Be aware that if you type it wrong, it's not going to find anything)"
    ConsoleReadOptions.productType <- Console.ReadLine()
    printfn ""

let rec StartProgram() =
      Conditions.allProviders <- true
      Conditions.allProducts <- true
      Conditions.allDates <- true
      Conditions.allStores <- true
      ConsoleReadOptions.week <- String.Empty
      ConsoleReadOptions.year <- String.Empty
      ConsoleReadOptions.provider <- String.Empty
      ConsoleReadOptions.productType <- String.Empty
      ConsoleReadOptions.shopcode <- String.Empty
      ConsoleReadOptions.shopname <- String.Empty
      printfn "Please, type the number on the left to select an option to calculate the cost:" 
      printfn "1. By stores"
      printfn "2. By dates"
      printfn "3. By producers"
      printfn "4. By product type"
      printfn "5. By date and shop"
      printfn "6. By date and producers"
      printfn "7. By date and producut type"
      printfn "8. By date, shop and product type"
      printfn "9. Everything"
      printfn ""

      let optionSelected = Console.ReadLine()
      printfn ""
      
      if optionSelected = "1" then
        GetShop()
      elif optionSelected = "2" then
        GetDate()
      elif optionSelected = "3" then
        GetProvider()
      elif optionSelected = "4" then
        GetProduct()
      elif optionSelected = "5" then
        GetDate()
        GetShop()
      elif optionSelected = "6" then
        GetDate()
        GetProvider()
      elif optionSelected = "7" then
        GetDate()
        GetProduct()
      elif optionSelected = "8" then
        GetDate()
        GetProduct()
        GetShop()
      elif optionSelected = "9" then
        printfn "Everything selected"
        printfn ""
      else
        selections.validOption <- false
        printfn "Please, select a valid option"

      if selections.validOption then
        let task = Task.Factory.StartNew(fun () -> 
                              Calculate()
                              printfn ""
                              printfn "Do you want to exit? Enter 'close' to exit, enter anything else to continue"
                              printfn ""
                              selections.finished <- Console.ReadLine()
                              printfn ""
                              )   
        task.Wait()
      else
        printfn ""
        selections.validOption <- true 
      
      if selections.finished <> "close" then
        StartProgram()
      else 
        0

[<EntryPoint>]
let main argv = 
    let storeCodesData = File.ReadAllLines(StoreFilesAndData.storeCodesFile)
    for i = 0 to storeCodesData.Length - 1 do        
         let storeDataSplit = storeCodesData.[i].Split(',')
         ShopNames.ShopCodes.Add(storeDataSplit.[1], storeDataSplit.[0])   

    let task = Task.Factory.StartNew(fun () -> StartProgram())
    task.Result
