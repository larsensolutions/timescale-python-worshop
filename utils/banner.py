from rich.console import Console
from rich.panel import Panel

ascii_art = r"""
                                        :-                                      
                             ..         #@.         .                           
                             .%:       -@@=        -+                           
                             -@%.      *@@#.      .@@.                          
                   :.        +@@+     .%@@@.     .#@@:                          
                   +#.       #@@@.    .@@@@:     -@@@-     ::                   
                   +@#.      #@@@=    .@@@@.     #@@@=    .%+                   
                   *@@*      +@@@*    .----     .@@@@:   .#@*                   
                   *@@@-     -%**=    .=%@%*.   .%#%%.   +@@#                   
                   =@@@#.     :==-.   +@@@@@+   :==:.   :@@@#                   
                   .@@@@:    -@@@@%: .%@@@@@* .#@@@@=   +@@@*                   
                    +*==.    *@@@@@#. =@@@@%: *@@@@@%  .%@@@=                   
                      -++-.  =@@@@@%.  :++-.  #@@@@@+  .===+.                   
                    .*@@@@*.  =%@@#: .:::.    :#@@%+..=#%%+                     
                    .@@@@@@=    :-=+#%@@@@%*=:  .:. .*@@@@@=                    
                    .*@@@@@=  :+#@@@@@@@@@@@@@#=.   .@@@@@@=                    
                     .+%@%+ :*@@@@@@@@@@@@@@@@@@@*-. *@@@@#.                    
          =#+.         ... +@@@@@@@@@@@@@@@@@@@@@@@#=.=*+-.       .=*+:         
          -%#.           .#@@@@@@@@@@@@@@@@@@@@@@@@@@%:           :%@@=         
           =%. .=+:     .#@@@@@@@@@@@@@@@@@@@@@@@@@@@@%.     .-==: +#.          
      .++- .@- =@%=     +@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@=     :@@@#.%= :==.      
      :%%#-:%#=*:.     .@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@*     .:=#+-@:.*@#.      
        ..-=#@*.       =@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%.       .+@@+=-.        
            :@%:    .-*@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%+:      =@+.           
             +@@*=+#%@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@#+-:-*@@.            
             .#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@-             
              .=*###*+-:%@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@-:+#%@@@%#-              
                     .. .#@@@@@@@@@@@@@@@@@@@@@@@@@@@%- .  .....                
         :--. ...  -%@@%#*@@@@@@@@@@@@@@@@@@@@@@@@@@@#*%%%*.  ..  .=+-.         
       ..%@@-.#@@+ #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@- +%%#.=@@@-         
     :#%#-=#:.%@*- *@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@- =#@#.:*--=*+:      
     -%@%**@@%@#   :@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@#.  :@@#@#+*@@@+      
      ....:=*@@@%*=-+@@@@@@@@@*##**%@@@@@@#+*#%##@@@@@@@@%--=*%@@%*=:..-:.      
             .+@@@@@@@@@@@@@@@#.   .-+##=:     -%@@@@@@@@@@@@@@%=.              
               .+%@@@@@@@@@@@@@#.             :@@@@@@@@@@@@@@%=.                
                 .=#@@@@@@@@@@@#.             -@@@@@@@@@@@@*-.                  
                    :=*#%@@@%*=.               :+%@@@@%#*-.                     
                        .....                    ..:::.                         
                                                                                """


def print_banner():
    console = Console()
    console.print(Panel(ascii_art, title="Grizzlyfrog", border_style="magenta"))

    console.print(
        Panel(
            """
    Welcome to the Timescale Python Workshop!

    In this workshop, you'll learn how to use TimescaleDB with Python to handle time-series data effectively.
    Follow the exercises to build your skills step-by-step.

    Objective:
    - Understand the basics of TimescaleDB and time-series data.
    - Learn how to set up and interact with TimescaleDB using Python.
    - Explore compression, hyperfunctions, continuous aggregates, and more.
    - Matplotlib visualization of time-series data.

    Use the CLI commands to navigate through the exercises and complete the tasks.

    Your efforts will be shared with the other participants and shown live on the big screen!
    Happy coding!
    """,
            title="Objective",
            border_style="magenta",
        )
    )


if __name__ == "__main__":
    print_banner()
