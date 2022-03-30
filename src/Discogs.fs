module Discogs

open System
open System.Net.Http
open System.Net.Http.Json
open System.Threading.Tasks

open Flurl
open FsToolkit.ErrorHandling
open FSharp.Data

type PaginatedResponse<'T> = {
    pagination: {|
        page: int
        pages: int
        per_page: int
        items: int
        urls: {|
            last: string
            next: string option
            previous: string option
        |}
    |}
    results: 'T[]
}

type SearchRequest = {
    Title: string
    Artist: string
    Year: int
}

type ReleaseInfo = {
    id: int
    title: string
    artists: {|
        name: string
    |}[]
    year: int
    images: {|
        ``type``: string
        uri: string
    |}[] option
    uri: string
    community: {|
        want: int
        have: int
    |}
}

type SearchError =
    | RequestError of string
    | NoResults

let getReleaseInfo (releaseId: int) (apiKey: string) (hc: HttpClient): Task<Result<ReleaseInfo, SearchError>> =
    task {
        let url =
            $"https://api.discogs.com/releases/%i{releaseId}"
                .SetQueryParam("token", apiKey)
        try
            let! response = hc.GetFromJsonAsync<ReleaseInfo>(url)
            return Ok response
        with
        | e -> return Error (RequestError e.Message)
    }

type SearchResponse = {
    id: int
}

let searchCdsAsync (searchRequest: SearchRequest) (apiKey: string) (hc: HttpClient): Task<Result<ReleaseInfo, SearchError>> =
    let rec searchCdsAsyncInner (usOnly: bool) =
        task {
            let url =
                "https://api.discogs.com/database/search"
                    .SetQueryParams(
                        {|
                            release_title = searchRequest.Title
                            artist = searchRequest.Artist
                            year = searchRequest.Year
                            ``type`` = "release"
                            format = "CD"
                            token = apiKey
                        |})
            let url =
                if usOnly then
                    url.SetQueryParam("country", "US")
                else
                    url
            try
                let! response = hc.GetFromJsonAsync<PaginatedResponse<SearchResponse>>(url)
                // We just take the first result if it exists and ignore the rest.
                // It would be possible to add more information to help disambiguate
                // but that would make inputing the data for the collection take
                // longer. If no results were found, we retry the search without
                // the US only restriction.
                let n = response.results.Length
                if n > 1 then
                    printfn $"WARNING: %s{searchRequest.Title} by %s{searchRequest.Artist} released in %i{searchRequest.Year} returned %i{n} results"
                match Array.toList response.results with
                | [] ->
                    if usOnly then
                        return! searchCdsAsyncInner false
                    else
                        return Error NoResults
                | [searchResponse]
                | searchResponse :: _ ->
                    return! getReleaseInfo searchResponse.id apiKey hc
            with
            | e ->
                printfn "%s" e.Message
                return Error (RequestError e.Message)
        }

    searchCdsAsyncInner true

// The API offers a price suggestion feature, but it returns wildly overprice
// results. Prices based on actual sales are available on the website itself but
// not through the API, so we have to extract them from the HTML.
let getPrice (doc: HtmlDocument) (url: string) (searchTerm: string) =
    result {
        let! medianContainer =
            doc.CssSelect("#release-stats li")
            |> List.find (fun el -> el.InnerText().Contains(searchTerm))
            |> fun el -> el.CssSelect("span")
            |> List.tryHead
            |> Result.requireSome $"Error finding median element at %s{url}"
        let medianText = medianContainer.InnerText().Trim().Replace("$", "")
        let! median =
            match Double.TryParse(medianText) with
            | true, n -> Ok n
            | false, _ -> Error $"Error converting %s{medianText} to number at %s{url}"
        return median
    }

type PriceStatistics = {
    Lowest: float
    Median: float
    Highest: float
}

let getPriceStatisticsAsync (url: string) (hc: HttpClient) =
    taskResult {
        try
            let! html = hc.GetStringAsync(url)
            let doc = HtmlDocument.Parse(html)
            let! lowest = getPrice doc url "Lowest"
            let! median = getPrice doc url "Median"
            let! highest = getPrice doc url "Highest"
            return {
                Lowest = lowest
                Median = median
                Highest = highest
            }
        with
        | e ->
            printfn "%s" e.Message
            return! Error $"Error parsing %s{url}"
    }

[<CLIMutable>]
type CdDescription = {
    Title: string
    Artist: string
    Year: int
}

type AppraisedCd = {
    Id: int
    Title: string
    Artist: string
    Year: int
    CoverImage: string option
    Uri: string
    Community: {|
        Want: int
        Have: int
    |}
    Prices: PriceStatistics
}

type AppraiseCdError =
    | SearchError of SearchError
    | GetMedianPriceError of string

let appraiseCdAsync (cd: CdDescription) (apiKey: string) (hc: HttpClient): Task<Result<AppraisedCd, AppraiseCdError>> =
    taskResult {
        let searchRequest: SearchRequest = {
            Title = cd.Title
            Artist = cd.Artist
            Year = cd.Year
        }
        let! releaseInfo =
            searchCdsAsync searchRequest apiKey hc
            |> TaskResult.mapError SearchError
        printfn "%s" releaseInfo.uri
        let! priceStatistics =
            getPriceStatisticsAsync releaseInfo.uri hc
            |> TaskResult.mapError GetMedianPriceError
        return {
            Id = releaseInfo.id
            Title = releaseInfo.title
            Artist = releaseInfo.artists[0].name
            Year = releaseInfo.year
            CoverImage =
                releaseInfo.images
                |> Option.map (
                    Array.tryFind (fun image -> image.``type`` = "primary")
                    >> Option.map (fun image -> image.uri))
                |> Option.flatten
            Uri = releaseInfo.uri
            Community = {|
                Want = releaseInfo.community.want
                Have = releaseInfo.community.have
            |}
            Prices = priceStatistics
        }
    }
