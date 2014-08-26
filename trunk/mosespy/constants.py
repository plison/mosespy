
from mosespy.system import Path

rootPath = Path(__file__).getUp().getUp()
expDir = rootPath + "/experiments/"
moses_root = Path(rootPath + "/moses")
mgizapp_root = Path(rootPath + "/mgizapp")
irstlm_root = Path(rootPath + "/irstlm")

decoder = moses_root+"/bin/moses"

defaultAlignment = "grow-diag-final-and"
defaultReordering = "msd-bidirectional-fe"

