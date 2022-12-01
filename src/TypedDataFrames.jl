module TypedDataFrames

using DataFrames

macro withcols(func)
    expr = Expr(:quote, func)

    functiondefinition = expr.args[1].args[1].args

    for i in 2:length(functiondefinition) # first is function name

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
                    requiredcols = [eval(c) for c in functionargument[2].args[2:end]]

                    # Remove the array from the type (DataFrame[:a, :b] -> DataFrame)
                    functionargument[2] = type

                    # Add an assertion check to the start of the function
                    df = functionargument[1]
                    pushfirst!(expr.args[1].args[2].args, :(
                        missingcols = setdiff($requiredcols, Symbol.(names($df)));
                        @assert isempty(missingcols) "Missing columns [" * join(missingcols, ", ") * ']';
                    ))

                end
            end
        end
    end

    return Expr(:call, GlobalRef(Base, :eval), __module__, expr)
end

export @withcols

end
