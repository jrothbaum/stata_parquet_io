capture program drop test_opt
program test_opt
    syntax [, nonstrict NONSTRICT2 lax]
    di "nonstrict=>`nonstrict'<"
    di "NONSTRICT2=>`nonstrict2'<"
    di "lax=>`lax'<"
end

test_opt, nonstrict
test_opt, lax
