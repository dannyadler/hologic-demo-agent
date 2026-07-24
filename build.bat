@echo off
REM Build the Hologic demo agent into double-click Windows executables.
REM Run once on the Windows device. Ship the .exe next to config.json + certs\.
REM
REM   HGDeviceConsole.exe  - operator GUI: onboarding wizard + device console (Q2)
REM   HGDeviceAgent.exe    - headless agent/service; supports --preflight and
REM                          --silent for unattended fleet rollout (Q6, Q2 silent)

python -m pip install --upgrade pyinstaller paho-mqtt certifi
if errorlevel 1 goto :err

pyinstaller --onefile --windowed --name HGDeviceConsole --hidden-import paho.mqtt.client agent_gui.py
if errorlevel 1 goto :err

pyinstaller --onefile --console --name HGDeviceAgent --hidden-import paho.mqtt.client agent.py
if errorlevel 1 goto :err

echo.
echo Built: dist\HGDeviceConsole.exe  (double-click)
echo        dist\HGDeviceAgent.exe    (HGDeviceAgent.exe --preflight ^| --silent)
echo.
echo Next: copy the exe(s) into a folder that also holds config.json and certs\,
echo       then double-click HGDeviceConsole.exe. No Python needed on that machine.
goto :eof

:err
echo.
echo BUILD FAILED - see the error above.
exit /b 1
