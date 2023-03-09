module TypedDataFrames

using DataFrames

macro withcols(func)
    functiondefinition = func.args[1].args

    # If has return type, first argument won't be a Symbol
    if typeof(functiondefinition[1]) == Expr
        functiondefinition = functiondefinition[1].args
    end

    for i in length(functiondefinition):-1:2 # first is function name

        if hasproperty(functiondefinition[i], :args)

            functionargument = functiondefinition[i]

            # after semicolon
            if functionargument.head == :parameters
                functionargument = functionargument.args[1]
            end

            # has no type
            if typeof(functionargument) == Symbol
                continue
            end

            # has default argument
            if functionargument.head == :kw
                functionargument = functionargument.args[1]
            end

            # has no type
            if typeof(functionargument) == Symbol
                continue
            end

            functionargument = functionargument.args

            if length(functionargument) > 1 && hasproperty(functionargument[2], :args)

                type = functionargument[2].args[1]

                if type in [:DataFrame, :AbstractDataFrame]

                    # Extract columns from type (DataFrame[:a, :b] -> [:a, :b])
                    requiredcols = [
                        typeof(c) == Expr ? c.args[2].value : c.value
                        for c in functionargument[2].args[2:end]
                    ]

                    requiredtypes = [
                        (c.args[2].value, string(c.args[3]))
                        for c in functionargument[2].args[2:end]
                        if typeof(c) == Expr
                    ]
                    requiredtypecols = first.(requiredtypes)

                    # Remove the array from the type (DataFrame[:a, :b] -> DataFrame)
                    functionargument[2] = type

                    # Add an assertion check to the start of the function
                    df = functionargument[1]

                    if !isempty(requiredtypecols)
                        # Can probably change this to a splat...
                        pushfirst!(func.args[2].args, quote
                            let
                                local invalidcolumntypes = [
                                    String(col) * ":- expected " * req * " != got " * is
                                    for ((col, req), is) in zip($requiredtypes, string.(eltype.(eachcol(df[!, $requiredtypecols]))))
                                    if req != is
                                ]
                                @assert isempty(invalidcolumntypes) "Invalid Column Types [\n " * join(invalidcolumntypes, ",\n ") * "\n]"
                            end
                        end)
                    end

                    pushfirst!(func.args[2].args, quote
                        let
                            local missingcols = setdiff($requiredcols, Symbol.(names($df)))
                            @assert isempty(missingcols) "Missing columns [" * join(missingcols, ", ") * ']'
                        end
                    end)

                end
            end
        end
    end

    esc(func)
end

export @withcols

end
