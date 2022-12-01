using DataFrames
using Test
using TypedDataFrames

@testset "Single Argument (AbstractDataFrame)" begin

    @withcols function singleargumentabstract(df::AbstractDataFrame[:a, :b])
        return df[1, :a]
    end

    @withcols function singleargumentabstractdefault(df::AbstractDataFrame[:a, :b]=DataFrame(a=[1], b=[1]))
        return df[1, :a]
    end
    
    # Required Columns
    @test singleargumentabstract(
        DataFrame(a=[1, 2], b=[3, 4])
    ) == 1

    @test singleargumentabstractdefault() == 1

    # Extra Columns
    @test singleargumentabstract(
        DataFrame(a=[1, 2], b=[3, 4], c=[5, 6])
    ) == 1

    @test singleargumentabstractdefault(
        DataFrame(a=[1, 2], b=[3, 4], c=[5, 6])
    ) == 1

    # Missing Column
    @test_throws AssertionError singleargumentabstract(
        DataFrame(a=[1, 2], c=[5, 6])
    )

    @test_throws AssertionError singleargumentabstractdefault(
        DataFrame(a=[1, 2], c=[5, 6])
    )

end

@testset "Single Argument" begin

    @withcols function singleargument(df::DataFrame[:a, :b])
        return df[1, :a]
    end
    
    # Required Columns
    @test singleargument(
        DataFrame(a=[1, 2], b=[3, 4])
    ) == 1

    # Extra Columns
    @test singleargument(
        DataFrame(a=[1, 2], b=[3, 4], c=[5, 6])
    ) == 1

    # Missing Column
    @test_throws AssertionError singleargument(
        DataFrame(a=[1, 2], c=[5, 6])
    )

end

@testset "DataFrame but not columns" begin

    @withcols function none1(df1::DataFrame[:a, :b], df2::DataFrame)
        return df1[1, :a]
    end

    @withcols function none2(df1::DataFrame, df2::DataFrame[:a, :b])
        return df2[1, :a]
    end
    
    # Required Columns
    @test none1(
        DataFrame(a=[1, 2], b=[3, 4]),
        DataFrame(c=[1, 2], d=[3, 4]),
    ) == 1

    @test none2(
        DataFrame(c=[1, 2], d=[3, 4]),
        DataFrame(a=[1, 2], b=[3, 4]),
    ) == 1

    # Extra Columns
    @test none1(
        DataFrame(a=[1, 2], b=[3, 4], e=[5, 6]),
        DataFrame(c=[1, 2], d=[3, 4]),
    ) == 1

    @test none2(
        DataFrame(c=[1, 2], d=[3, 4]),
        DataFrame(a=[1, 2], b=[3, 4], e=[5, 6]),
    ) == 1

    # Missing Column
    @test_throws AssertionError none1(
        DataFrame(a=[1, 2], e=[5, 6]),
        DataFrame(c=[1, 2], d=[3, 4]),
    )

    @test_throws AssertionError none2(
        DataFrame(c=[1, 2], d=[3, 4]),
        DataFrame(a=[1, 2], e=[5, 6]),
    )

end

@testset "Multi-Argument" begin

    @withcols function multiargument(df::DataFrame[:a, :b], f::Int64)
        return df[1, :a]
    end

    @withcols function multiargument2(f::Int64, df::DataFrame[:a, :b])
        return df[1, :a]
    end

    @withcols function multiargument3(f, df::DataFrame[:a, :b])
        return df[1, :a]
    end

    # Required Columns
    @test multiargument(
        DataFrame(a=[1, 2], b=[3, 4]),
        5,
    ) == 1

    @test multiargument2(
        5,
        DataFrame(a=[1, 2], b=[3, 4]),
    ) == 1

    @test multiargument3(
        5,
        DataFrame(a=[1, 2], b=[3, 4]),
    ) == 1

    # Extra Columns
    @test multiargument(
        DataFrame(a=[1, 2], b=[3, 4], c=[5, 6]),
        5,
    ) == 1

    @test multiargument2(
        5,
        DataFrame(a=[1, 2], b=[3, 4], c=[5, 6]),
    ) == 1

    @test multiargument3(
        5,
        DataFrame(a=[1, 2], b=[3, 4], c=[5, 6]),
    ) == 1

    # Missing Column
    @test_throws AssertionError multiargument(
        DataFrame(a=[1, 2], c=[5, 6]),
        5,
    )

    @test_throws AssertionError multiargument2(
        5,
        DataFrame(a=[1, 2], c=[5, 6]),
    )

    @test_throws AssertionError multiargument3(
        5,
        DataFrame(a=[1, 2], c=[5, 6]),
    )

end

@testset "Multi-DataFrame" begin

    @withcols function multidataframe(df1::DataFrame[:a, :b], df2::DataFrame[:c])
        return df1[1, :a] + df2[1, :c]
    end

    @withcols function multidataframe2(df1::DataFrame[:a, :b], df2::DataFrame[:c]=DataFrame(c=[3,4,5]))
        return df1[1, :a] + df2[1, :c]
    end

    # Required Columns
    @test multidataframe(
        DataFrame(a=[1, 2], b=[3, 4]),
        DataFrame(c=[5, 6]),
    ) == 6

    @test multidataframe2(
        DataFrame(a=[1, 2], b=[3, 4])
    ) == 4

    # Extra Columns
    @test multidataframe(
        DataFrame(a=[1, 2], b=[3, 4], x=[8, 9]),
        DataFrame(c=[5, 6]),
    ) == 6
     
    @test multidataframe(
        DataFrame(a=[1, 2], b=[3, 4]),
        DataFrame(c=[5, 6], x=[8, 9]),
    ) == 6

    # Missing Column
    @test_throws AssertionError multidataframe(
        DataFrame(a=[1, 2]),
        DataFrame(c=[5, 6]),
    )

    @test_throws AssertionError multidataframe(
        DataFrame(a=[1, 2], b=[3, 4]),
        DataFrame(x=[5, 6]),
    )

    @test_throws AssertionError multidataframe2(
        DataFrame(b=[3, 4]),
    )

end

@testset "With Semicolon" begin

    @withcols function withsemicolon(df::DataFrame[:a, :b]; a)
        return df[1, :a]
    end

    @withcols function withsemicolon2(df::DataFrame[:a, :b]; a=3)
        return df[1, :a]
    end
    
    # Required Columns
    @test withsemicolon(
        DataFrame(a=[1, 2], b=[3, 4]);
        a=1
    ) == 1

    @test withsemicolon2(
        DataFrame(a=[1, 2], b=[3, 4])
    ) == 1

    # Extra Columns
    @test withsemicolon(
        DataFrame(a=[1, 2], b=[3, 4], c=[5, 6]);
        a=1
    ) == 1

    @test withsemicolon2(
        DataFrame(a=[1, 2], b=[3, 4], c=[5, 6])
    ) == 1

    # Missing Column
    @test_throws AssertionError withsemicolon(
        DataFrame(a=[1, 2], c=[5, 6]);
        a=1
    )

    @test_throws AssertionError withsemicolon2(
        DataFrame(a=[1, 2], c=[5, 6])
    )

end
 
@testset "In Semicolon" begin

    @withcols function insemicolon(a; df::DataFrame[:a, :b])
        return df[1, :a]
    end

    @withcols function insemicolon2(a; df::DataFrame[:a, :b]=DataFrame(a=[1,2], b=[3,4]))
        return df[1, :a]
    end
    
    # Required Columns
    @test insemicolon(
        3;
        df=DataFrame(a=[1, 2], b=[3, 4])
    ) == 1

    @test insemicolon2(
        3;
        df=DataFrame(a=[1, 2], b=[3, 4])
    ) == 1

    @test insemicolon2(
        3;
    ) == 1

    # Extra Columns
    @test insemicolon(
        3;
        df=DataFrame(a=[1, 2], b=[3, 4], c=[5, 6])
    ) == 1

    @test insemicolon2(
        3;
        df=DataFrame(a=[1, 2], b=[3, 4], c=[5, 6])
    ) == 1

    @test insemicolon2(
        3;
    ) == 1

    # Missing Column
    @test_throws AssertionError insemicolon(
        3;
        df=DataFrame(a=[1, 2], c=[5, 6])
    )

    @test_throws AssertionError insemicolon2(
        3;
        df=DataFrame(a=[1, 2], c=[5, 6])
    )

end

@testset "Single Line" begin

    @withcols singleline(a::DataFrame[:a]) = a[1, :a]
    
    # Required Columns
    @test singleline(DataFrame(a=[1, 2])) == 1

    # Extra Columns
    @test singleline(DataFrame(a=[1, 2], b=[3,4])) == 1

    # Missing Column
    @test_throws AssertionError singleline(DataFrame(b=[3,4]))

end
