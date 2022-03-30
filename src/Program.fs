open System
open System.IO
open System.Net.Http
open System.Net.Http.Headers
open System.Text.Json
open System.Text.Json.Serialization
open System.Globalization

open FsToolkit.ErrorHandling
open CsvHelper

open Discogs

let loadDescriptions (inputFile: string) =
    use reader = new StreamReader(inputFile)
    use csv = new CsvReader(reader, CultureInfo.InvariantCulture)
    let rows = csv.GetRecords<CdDescription>()
    Array.ofSeq rows

let options = JsonSerializerOptions(WriteIndented = true)
options.Converters.Add(JsonFSharpConverter())

type SavedData = CdDescription * Result<AppraisedCd,AppraiseCdError>

let saveDataAsync (dataFile: string) (data: SavedData array) =
    task {
        use stream = File.OpenWrite(dataFile)
        do! JsonSerializer.SerializeAsync(stream, data, options)
    }

let loadDataAsync (dataFile: string) =
    task {
        use stream = File.OpenRead(dataFile)
        return! JsonSerializer.DeserializeAsync<SavedData array>(stream, options)
    }

let getDataAsync (inputFile: string) (dataFile: string) (apiKey: string) (hc: HttpClient) =
    task {
        let descriptions = loadDescriptions inputFile
        let data =
            descriptions
            |> Array.mapi (fun i cd ->
                task {
                    printfn $"Processing %s{cd.Title} (%i{i + 1} of %i{descriptions.Length})..."
                    let! appraisal = appraiseCdAsync cd apiKey hc
                    // Rate limit is 60 requests per minute (appraiseCdAsync makes multiple requests)
                    do! Async.Sleep(TimeSpan.FromSeconds(3))
                    return cd, appraisal
                } |> Async.AwaitTask
                |> Async.RunSynchronously)
        do! saveDataAsync dataFile data
    }

// CsvHelper doesn't understand Option
type OutputRow = {
    MedianPrice: Nullable<float>
    Title: string
    Artist: String
    Year: int
    Url: string
}

let writeOutput (outputFile: string) (data: OutputRow[]) =
    use writer = new StreamWriter(outputFile)
    use csv = new CsvWriter(writer, CultureInfo.InvariantCulture)
    csv.WriteRecords(data)

module Result =
    let unwrap (res: Result<'A,'B>) =
        match res with
        | Ok x -> x
        | Error _ -> failwith "Can't unwrap an Error"

let processData (data: SavedData array) =
    let oks, errors = Array.partition (snd >> Result.isOk) data

    [|
        yield!
            oks
            |> Array.map (snd >> Result.unwrap)
            |> Array.sortByDescending (fun cd -> cd.Prices.Median)
            |> Array.map (fun cd ->
                {
                    MedianPrice = Nullable(cd.Prices.Median)
                    Title = cd.Title
                    Artist = cd.Artist
                    Year = cd.Year
                    Url = cd.Uri
                })
        yield!
            errors
            |> Array.map fst
            |> Array.sortByDescending (fun cd -> cd.Title)
            |> Array.map (fun cd ->
                {
                    MedianPrice = Nullable()
                    Title = cd.Title
                    Artist = cd.Artist
                    Year = cd.Year
                    Url = null
                })
    |]

[<EntryPoint>]
let main args =
    let apiKey = Environment.GetEnvironmentVariable("DISCOGS_API_KEY")
    if String.IsNullOrEmpty(apiKey) then
        failwith "Environment variable DISCOGS_API_KEY must be set"

    let dataFile = "data.json"
    let inputFile = "CDs.csv"
    let outputFile = "Prices.csv"

    let hc = new HttpClient()
    hc.DefaultRequestHeaders.UserAgent.Add(ProductInfoHeaderValue("CannonCdPrices", "1.0"))

    task {
        if not (File.Exists(dataFile)) then
            printfn "Loading data from API"
            do! getDataAsync inputFile dataFile apiKey hc
        else
            printfn "Loading cached data from disk"
        let! data = loadDataAsync dataFile
        printfn "Writing prices to disk"
        let outputRows = processData data
        writeOutput outputFile outputRows
    } |> Async.AwaitTask
    |> Async.RunSynchronously

    0
