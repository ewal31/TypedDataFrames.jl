# Typed DataFrames

This package exports a macro `@withcols` that allows one to add required columns
to the DataFrame type within a function annotation. This is just a wrapper around
the julia [DataFrames package](https://github.com/JuliaData/DataFrames.jl)

For example

```julia
using DataFrames
using TypedDataFrames

# This function requires a dataframe with the columns :a and :b
@withcols function sumfirst(df::AbstractDataFrame[:a, :b])
    return df[1, :a] + df[1, :b]
end

sumfirst(DataFrame(a=[1,2], b=[3,4])) # 4

# Can have extra columns
sumfirst(DataFrame(a=[1,2], b=[3,4], c=[5,6])) # 4

# When a column is missing, throws an AssertionError
sumfirst(DataFrame(a=[1,2], c=[5,6]))
# ERROR: AssertionError: Missing columns [b]
```
