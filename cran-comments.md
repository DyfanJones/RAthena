Initial release of RAthena package

# Test environments

* local OS X install, R 3.5.2

# R CMD check results (local)
0 errors | 0 warnings | 0 notes 

# R devtools::check_rhub() results
0 errors | 0 warnings | 1 note

checking CRAN incoming feasibility ... NOTE
  Maintainer: 'Dyfan Jones <dyfan.r.jones@gmail.com>'
  
  
  License components with restrictions and base license permitting such:
    MIT + file LICENSE
  File 'LICENSE':
    
  New submission
    Copyright (c) 2019 Dyfan Jones
    MIT License
    Permission is hereby granted, free of charge, to any person obtaining a copy
    
    in the Software without restriction, including without limitation the rights
    of this software and associated documentation files (the "Software"), to deal
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:
    
    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.
    
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
  
    SDK (3:38, 7:96)
    Boto (3:32, 7:64)
  Possibly mis-spelled words in DESCRIPTION:
  

* Author Notes: 
** `Boto` is refering to python package `Boto3` this is not a spelling error
** `SDK` is refering to software development kit abbreviation

# unit tests (using testthat) results
OK:       19
Failed:   0
Warnings: 0
Skipped:  0