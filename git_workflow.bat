@echo off
cd /d "D:\Học tập\Năm_3\PhanTichDuLieuLon\FinalProject"

echo ===== STEP 1: Git Status and Current Branch =====
git --no-pager status
echo.
echo Current Branch:
git rev-parse --abbrev-ref HEAD
echo.

echo ===== STEP 2: Create/Switch to Modeling Branch =====
git show-ref --quiet refs/heads/Modeling
if errorlevel 1 (
    echo Modeling branch does not exist. Creating from current HEAD...
    git checkout -b Modeling
) else (
    echo Modeling branch exists. Switching to it...
    git checkout Modeling
)
git rev-parse --abbrev-ref HEAD
echo.

echo ===== STEP 3: Stage All Changes =====
git add -A
echo.

echo ===== STEP 4: Check Status Before Commit =====
git --no-pager status
echo.

echo ===== STEP 5: Create Commit =====
git commit -m "Fix pipeline pathing, typing compatibility, and events schema" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
if errorlevel 1 (
    echo Commit failed or nothing to commit
)
echo.

echo ===== STEP 6: Get Commit Hash =====
git rev-parse HEAD
echo.

echo ===== STEP 7: Push to Origin =====
git push -u origin Modeling
echo.

echo ===== FINAL SUMMARY =====
echo Branch: 
git rev-parse --abbrev-ref HEAD
echo Commit Hash:
git rev-parse HEAD
