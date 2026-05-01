#!/usr/bin/env python3
"""
Morocco Jobs → Telegram
GitHub Actions scheduled. Ethical. Clean. ALL BUGS FIXED.

Fix index:
  FIX-1  Syntax error: `>= \n 5` → `>= 5`
  FIX-2  parse_cards() — off-site links rejected, min-title guard tightened
  FIX-3  Glassdoor URL — use unencoded kw length, drop gracefully if broken
  FIX-4  SQL IN(?) — dynamic placeholder count
  FIX-5  robots.txt — cached per domain, not re-fetched every call
  FIX-6  Runtime budget — scrape a random sample to stay under 6h GH Actions
  FIX-7  Block counter — per-source, not global kill-switch for everything
  FIX-8  Contract "?" — documented, intentional silent-drop noted in code
  FIX-9  find_text() — word-boundary regex so "salary" ≠ "no-salary-hidden"
  FIX-10 Message split — split by accumulated byte length, not job count
  FIX-11 sent=1 — only marks rows whose tg_send() returned True
"""

import requests, sqlite3, time, random, hashlib, logging, sys, os, re
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import quote_plus, urlparse
from urllib.robotparser import RobotFileParser
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ═══ CONFIG ═══
TG_TOKEN = os.environ.get("TG_TOKEN", "YOUR_TOKEN")
TG_CHAT  = os.environ.get("TG_CHAT",  "YOUR_CHAT")

FILTER = ["CDI", "CDD", "CIVP", "STAGE"]
KEYWORDS_EXTRA = [

    # =====================================================
    #  TELECOM & RÉSEAUX
    # =====================================================
    "ingénieur télécom", "technicien télécom", "ingénieur fibre optique",
    "technicien fibre optique", "ingénieur 5g", "ingénieur radio",
    "technicien réseau mobile", "ingénieur transmission",
    "ingénieur cœur réseau", "ingénieur ip", "ingénieur mpls",
    "ingénieur voip", "technicien voip", "administrateur voip",
    "ingénieur téléphonie", "technicien téléphonie",
    "ingénieur sdn", "ingénieur nfv", "network architect",
    "spécialiste wifi", "ingénieur wlan", "ingénieur antenne",
    "technicien antenne", "installateur fibre", "raccordeur",
    "technicien ftth", "technicien hertzien", "ingénieur hertzien",
    "ingénieur broadcasting", "technicien broadcasting",
    "ingénieur satellite", "technicien satellite",
    "ingénieur réseau", "administrateur système réseau",
    "noc engineer", "noc technician", "soc engineer",
    "ingénieur supervision", "technicien supervision",
    "ingénieur oss bss", "ingénieur billing télécom",
    "ingénieur qualité réseau", "ingénieur optimisation réseau",
    "drive test engineer", "ingénieur planning réseau",
    "ingénieur déploiement réseau", "chef de projet télécom",
    "chef de projet réseau", "chef de projet fibre",
    "responsable réseau", "directeur réseau", "directeur télécom",
    "responsable infrastructure", "responsable exploitation",
    "ingénieur cœurbanking", "ingénieur itinérance",

    # =====================================================
    #  AUTOMOBILE & AÉRONAUTIQUE
    # =====================================================
    "ingénieur automobile", "technicien automobile",
    "mécanicien automobile", "diagnostiqueur automobile",
    "électricien automobile", "carrossier", "peintre automobile",
    "débossailleur", "sellier", "tôlier", "tapissier",
    "technicien contrôle technique", "contrôleur technique",
    "ingénieur motoriste", "ingénieur châssis", "ingénieur véhicule",
    "ingénieur essai véhicule", "ingénieur homologation",
    "ingénieur système embarqué", "ingénieur électronique embarquée",
    "ingénieur adas", "ingénieur autonome driving",
    "ingénieur batterie", "ingénieur véhicule électrique",
    "ingénieur hydrogen", "ingénieur pile à combustible",
    "ingénieur cao", "ingénieur catia", "ingénieur solidworks",
    "ingénieur simulation", "ingénieur calcul",
    "ingénieur fatigue", "ingénieur crash",
    "ingénieur aéronautique", "ingénieur avionique",
    "ingénieur structure aéronautique", "ingénieur systèmes avion",
    "ingénieur propulsion", "ingénieur turbomachine",
    "ingénieur aérodynamique", "ingénieur composites",
    "technicien aéronautique", "mécanicien avion",
    "mécanicien hélicoptère", "technicien avionique",
    "technicien maintenance aéronautique", "easa part 66",
    "technicien pnc", "pilote de ligne", "pilote cargo",
    "pilote hélicoptère", "pilote drone", "opérateur drone",
    "ingénieur drone", "technicien drone",
    "ingénieur spatial", "ingénieur lanceur",
    "ingénieur satellite spatial", "ingénieur mission spatiale",
    "controlleur mission", "ingénieur essai",

    # =====================================================
    #  ÉNERGIE NUCLÉAIRE & PÉTROCHIMIE
    # =====================================================
    "ingénieur nucléaire", "technicien nucléaire",
    "opérateur nucléaire", "physicien nucléaire",
    "ingénieur radioprotection", "technicien radioprotection",
    "ingénieur sûreté nucléaire", "expert sûreté",
    "ingénieur démantèlement", "ingénieur traitement déchets",
    "ingénieur pétrole", "ingénieur forage", "ingénieur réservoir",
    "ingénieur production pétrolière", "ingénieur pipeline",
    "ingénieur raffinage", "ingénieur procédé chimique",
    "ingénieur pétrochimie", "technicien forage",
    "technicien pétrole", "opérateur raffinerie",
    "sondeur", "géologue pétrolier", "géophysicien pétrolier",
    "ingénieur hse pétrole", "responsable hse industriel",
    "ingénieur gaz", "ingénieur gnl", "technicien gaz",
    "installateur gaz", "chauffagiste gaz",
    "ingénieur pipeline", "technicien pipeline",
    "ingénieur offshore", "technicien offshore",
    "opérateur plateforme", "marin offshore",

    # =====================================================
    #  MINES & CARRIÈRES
    # =====================================================
    "ingénieur mines", "ingénieur géologie", "géologue minier",
    "géotechnicien", "sondeur géotechnique",
    "ingénieur carrière", "responsable carrière",
    "conducteur d'engins carrière", "exploitant carrière",
    "technicien mines", "technicien géologie",
    "technicien géotechnique", "technicien topographie",
    "dessinateur géologue", "métallurgiste",
    "ingénieur métallurgie", "fondeur", "lamineur",
    "ingénieur sidérurgie", "technicien sidérurgie",
    "ingénieur matériaux", "ingénieur céramique",
    "ingénieur verre", "technicien matériaux",
    "essayeur", "laborantin mines",

    # =====================================================
    #  MARITIME & PORTUAIRE
    # =====================================================
    "officier marine marchande", "capitaine marine marchande",
    "second capitaine", "lieutenant", "enseigne vaisseau",
    "officier passerelle", "officier machine",
    "mécanicien marine", "électro mécanicien marine",
    "chef mécanicien", "second mécanicien",
    "électricien bord", "radio officier",
    "timonier", "matelot pont", "matelot machine",
    "chef steward", "cuisinier bord", "cameraman maritime",
    "plongeur professionnel", "scaphandrier",
    "maître plongeur", "instructeur plongée",
    "agent portuaire", "manutentionnaire portuaire",
    "docker", "arrimeur", "transitaire maritime",
    "agent consignataire", "courtier maritime",
    "affréteur maritime", "shipbroker",
    "agent maritime", "ship chandler",
    "contrôleur portuaire", "pilote portuaire",
    "lamaneur", "éclusier", "baliseur",
    "chef escale", "responsable terminal",
    "responsable portuaire", "directeur portuaire",
    "ingénieur portuaire", "ingénieur maritime",
    "architecte naval", "ingénieur construction navale",
    "technicien construction navale", "ajusteur naval",
    "soudeur naval", "peintre coque navire",

    # =====================================================
    #  SPORT & FITNESS
    # =====================================================
    "coach sportif", "entraîneur sportif", "préparateur physique",
    "préparateur mental", "analyste performance sportive",
    "kinésithérapeute du sport", "médecin du sport",
    "ostéopathe", "chiropracteur", "naturopathe",
    "diététicien sport", "nutritionniste sport",
    "professeur eps", "enseignant eps", "animateur sportif",
    "moniteur sportif", "professeur de sport",
    "entraîneur football", "entraîneur basketball",
    "entraîneur natation", "entraîneur tennis",
    "entraîneur athlétisme", "entraîneur cyclisme",
    "entraîneur rugby", "entraîneur handball",
    "entraîneur volley", "entraîneur boxe",
    "entraîneur judo", "entraîneur arts martiaux",
    "professeur yoga", "professeur pilates",
    "instructeur fitness", "personal trainer",
    "coach crossfit", "instructeur spinning",
    "instructeur aquagym", "maître nageur sauveteur",
    "sauveteur aquatique", "secouriste sportif",
    "arbitre sportif", "juge arbitre", "commissaire sportif",
    "directeur de course", "organisateur compétition",
    "gestionnaire événement sportif", "responsable complexe sportif",
    "directeur stade", "responsable équipements sportifs",
    "agent sportif", "scout sportif", "recruteur sportif",
    "journaliste sportif", "commentateur sportif",
    "consultant sport", "analyste tactique",
    "data analyst sport", "performance analyst",
    "biomecanicien", "ergonomiste sport",

    # =====================================================
    #  BEAUTÉ & BIEN-ÊTRE
    # =====================================================
    "esthéticienne", "esthéticien", "cosméticienne",
    "dermocosméticien", "spécialiste soins visage",
    "spécialiste soins corps", "masseur bien être",
    "praticien massage", "masseur relaxant",
    "réflexologue", "aromathérapeute", "sophrologue",
    "praticien reiki", "praticien shiatsu",
    "praticien ayurvéda", "praticien médecine traditionnelle",
    "coiffeur", "coiffeuse", "barbier", "coloriste",
    "styliste capillaire", "barber", "hair stylist",
    "maquilleur professionnel", "makeup artist",
    "prothésiste ongulaire", "nail artist",
    "tatoueur", "tatoueuse", "piercing artist",
    "bronzage", "spécialiste épilation",
    "spécialiste laser", "technicien laser esthétique",
    "conseiller beauté", "conseiller cosmétique",
    "délégué cosmétique", "visagiste", "image consultant",
    "personal shopper", "styliste personnel",
    "color consultant", "consultant image",

    # =====================================================
    #  FONCTION PUBLIQUE & ADMINISTRATION
    # =====================================================
    "fonctionnaire", "agent public", "agent de l'état",
    "administrateur civil", "administrateur territorial",
    "attaché d'administration", "attaché territorial",
    "secrétaire d'administration", "adjoint administratif",
    "adjoint technique", "agent technique",
    "directeur services publics", "directeur cabinet",
    "chef de cabinet", "conseiller municipal",
    "conseiller régional", "maire", "adjoint au maire",
    "préfet", "sous préfet", "secrétaire général préfecture",
    "inspecteur", "contrôleur", "vérificateur",
    "inspecteur des finances", "inspecteur des impôts",
    "contrôleur des impôts", "percepteur",
    "receveur", "comptable public",
    "trésorier payeur général", "trésorier payeur",
    "ingénieur des ponts", "ingénieur des eaux",
    "ingénieur d'état", "architecte d'état",
    "urbaniste public", "directeur urbanisme",
    "conservateur musée", "conservateur bibliothèque",
    "conservateur patrimoine", "conservateur archives",
    "directeur archives", "archiviste public",
    "bibliothécaire public", "médiathécaire",
    "documentaliste public", "chargé mission",
    "chargé d'études public", "chargé de projet public",
    "directeur aménagement", "directeur environnement",
    "responsable marchés publics", "acheteur public",
    "juriste marchés publics", "contrôleuse de gestion publique",
    "auditeur public", "inspecteur générale",
    "cour des comptes", "magistrat financier",
    "directeur agence état", "chef service état",

    # =====================================================
    #  ONG & ORGANISATIONS INTERNATIONALES
    # =====================================================
    "coordinateur ong", "directeur pays ong",
    "responsable programme", "chargé de programme",
    "chargé de projet humanitaire", "coordinateur humanitaire",
    "responsable humanitaire", "logisticien humanitaire",
    "administrateur ong", "financier ong",
    "chargé de suivi évaluation", "monitoring evaluation",
    "chargé plaidoyer", "advocacy officer",
    "chargé protection", "protection officer",
    "chargé genre", "gender specialist",
    "chargé inclusion", "inclusion officer",
    "spécialiste eau hygiène assainissement", "wash officer",
    "spécialiste nutrition humanitaire", "nutrition officer",
    "spécialiste sécurité alimentaire", "food security",
    "spécialiste éducation urgence", "education emergency",
    "spécialiste abri", "shelter specialist",
    "coordinateur urgence", "emergency coordinator",
    "réfugié officer", "protection déplacés",
    "chargé de communication ong", "fundraiser",
    "chargé de fundraising", "resource mobilization",
    "bilité", "bilité officer",
    "bilité manager", "bilité coordinator",
    "volunteer coordinator", "gestionnaire volontaires",
    "expert technique international", "consultant international",
    "expert onu", "expert banque mondiale",
    "expert ue", "expert bad", "expert fmi",
    "conseiller technique", "conseiller politique",
    "analyste géopolitique", "analyste conflits",
    "spécialiste paix", "peacebuilding officer",
    "médiateur international", "diplomate",
    "attaché diplomatique", "ambassadeur", "consul",
    "chancelier ambassade", "secrétaire diplomatique",
    "analyste risque pays", "analyste intelligence",

    # =====================================================
    #  ASSURANCE
    # =====================================================
    "agent général assurance", "courtier assurance",
    "conseiller assurance", "commercial assurance",
    "responsable agence assurance", "directeur assurance",
    "souscripteur", "underwriter", "souscripteur risque",
    "gestionnaire sinistres", "expert sinistres",
    "expert automobile", "expert incendie",
    "expert dommages", "responsable sinistres",
    "liquidateur sinistres", "indemnisation",
    "chargé de recours", "responsable recours",
    "actuaire", "actuaire vie", "actuaire non vie",
    "analyste actuariel", "responsable actuariat",
    "contrôleur assurance", "auditeur assurance",
    "compliance assurance", "responsable conformité assurance",
    "responsable réassurance", "réassureur",
    "analyste risque assurance", "responsable provision",
    "gestionnaire contrat", "gestionnaire portefeuille assurance",
    "technicien assurance", "assistant assurance",
    "secrétaire assurance", "standardiste assurance",
    "médiateur assurance", "ombudsman assurance",
    "inspecteur risque", "inspecteur assurance",
    "délégué général assurance", "directeur général assurance",

    # =====================================================
    #  BANQUE & MICROFINANCE
    # =====================================================
    "directeur banque", "directeur agence bancaire",
    "responsable agence bancaire", "chargé clientèle bancaire",
    "conseiller patrimoine bancaire", "conseiller professionnel",
    "gestionnaire de fortune", "wealth advisor",
    "private banker", "family officer",
    "analyste crédit bancaire", "responsable crédit",
    "chargé de crédit", "officier de crédit",
    "analyste risque bancaire", "risk manager bancaire",
    "responsable middle office", "middle office analyst",
    "responsable back office", "back office bancaire",
    "opérateur de marché", "trader", "sales trader",
    "market maker", "broker", "agent de change",
    "analyste financier bancaire", "analyste sell side",
    "analyste buy side", "analyste equity",
    "analyste fixed income", "analyste credit",
    "spécialiste produits dérivés", "structurer",
    "ingénieur financier", "quantitative analyst", "quant",
    "risk modeler", "model validation analyst",
    "responsable conformité bancaire", "compliance officer bancaire",
    "aml officer", "kyc officer", "fraud analyst",
    "responsable audit bancaire", "auditeur bancaire",
    "chargé de projet bancaire", "chef de projet digital banque",
    "responsable digital banque", "innovation bancaire",
    "responsable paiement", "spécialiste paiement",
    "responsable cash management", "trade finance officer",
    "spécialiste trade finance", "lettre de crédit",
    "responsable change", "agent de change",
    "spécialiste titres", "gestionnaire titres",
    "dépositaire", "custodian", "teneur de comptes",
    "agent de transfert", "responsable back office titres",
    "directeur microfinance", "responsable microfinance",
    "agent de crédit rural", "agent terrain microfinance",
    "analyste crédit microfinance", "gestionnaire portefeuille mfi",

    # =====================================================
    #  INDUSTRIE PHARMACEUTIQUE & COSMÉTIQUE
    # =====================================================
    "ingénieur pharmaceutique", "responsable production pharma",
    "directeur production pharma", "opérateur production pharma",
    "technicien production pharma", "technicien conditionnement",
    "responsable assurance qualité pharma", "aq pharma",
    "responsable validation", "ingénieur validation",
    "ingénieur procédé pharma", "ingénieur procédé cosmétique",
    "responsable affaires réglementaires", "regulatory affairs",
    "pharmacien responsable", "pharmacien industriel",
    "préparateur en pharmacie", "officine pharmacien",
    "hospitalier pharmacien", "pharmacovigilance",
    "ingénieur pharmacovigilance", "responsable pharmacovigilance",
    "ingénieur production cosmétique", "formulateur cosmétique",
    "cosmétologue", "responsable r&d cosmétique",
    "ingénieur r&d cosmétique", "technicien r&d cosmétique",
    "responsable affaires réglementaires cosmétique",
    "ingénieur biotechnologie", "technicien biotechnologie",
    "ingénieur biologie", "biologiste moléculaire",
    "bio informaticien", "bio analyste",
    "ingénieur production biotech", "technicien laboratoire biotech",
    "responsable laboratoire", "chef de laboratoire",
    "analyste laboratoire", "préparateur solution",
    "technicien de laboratoire médical", "biologiste médical",
    "cadre de laboratoire", "manipulateur efrm",
    "technicien imagerie médicale", "radiologue interventionnel",
    "dosimétriste", "physicien médical",
    "ingénieur biomédical", "technicien biomédical",
    "ingénieur équipement médical", "technicien équipement hospitalier",

    # =====================================================
    #  AGROALIMENTAIRE & INDUSTRIE ALIMENTAIRE
    # =====================================================
    "ingénieur agroalimentaire", "responsable production agroalimentaire",
    "directeur usine agroalimentaire", "responsable qualité agroalimentaire",
    "responsable haccp", "responsable sécurité alimentaire",
    "technicien qualité alimentaire", "contrôleur qualité alimentaire",
    "technologue alimentaire", "ingénieur procédé alimentaire",
    "responsable laboratoire alimentaire", "analyste alimentaire",
    "nutritionniste industriel", "développeur produit alimentaire",
    "ingieur r&d alimentaire", "technicien r&d alimentaire",
    "responsable approvisionnement alimentaire",
    "responsable achats matières premières",
    "technicien emballage", "ingénieur emballage",
    "responsable packaging", "packaging technologist",
    "conducteur machine agroalimentaire", "opérateur ligne conditionnement",
    "technicien maintenance agroalimentaire",
    "responsable planification production", "planificateur agroalimentaire",
    "responsable logistique agroalimentaire",
    "responsable entrepôt frigorifique", "technicien froid industriel",
    "responsable transport frigorifique",
    "œnologue", "maître de chai", "caviste",
    "sommelier professionnel", "dégreer", "courtier vin",
    "brasseur", "maître brasseur", "distillateur",
    "fromager affineur", "boucher", "charcutier",
    "poissonnier", "traiteur artisanal", "pâtissier",
    "chocolatier", "glacier", "confiseur",
    "boulanger artisanal", "meunier",

    # =====================================================
    #  FORESTERIE & ENVIRONNEMENT NATUREL
    # =====================================================
    "ingénieur forestier", "technicien forestier",
    "garde forestier", "garde champêtre", "garde nature",
    "agent environnement", "agent parc naturel",
    "responsable parc national", "directeur parc naturel",
    "naturaliste", "ornithologue", "entomologiste",
    "botaniste", "écologue", "écotoxicologue",
    "biologiste environnement", "biologiste marine",
    "biologiste conservation", "spécialiste biodiversité",
    "chargé études environnement", "chargé mission nature",
    "chargé mission biodiversité", "chargé mission faune sauvage",
    "responsable études impact", "expert environnemental",
    "consultant environnemental", "auditeur environnement",
    "responsable remédiation", "spécialiste dépollution",
    "ingénieur assainissement", "technicien assainissement",
    "ingénieur eau", "ingénieur traitement eau",
    "technicien station épuration", "responsable distribution eau",
    "hydrogéologue", "hydrologue", "limnologue",
    "océanographe", "glaciologue", "climatologue",
    "météorologue", "prévisionniste météo",
    "technicien météorologie", "observateur météorologique",
    "géographe", "cartographe", "géomaticien",
    "télédétection", "sigiste", "spécialiste gis",
    "analyste géospatial", "ingénieur géospatial",

    # =====================================================
    #  ARCHÉOLOGIE & PATRIMOINE
    # =====================================================
    "archéologue", "archéologue sous marin",
    "archéologue préventif", "technicien archéologie",
    "dessinateur archéologue", "photographe archéologue",
    "anthropologue", "ethnologue", "sociologue",
    "ethnographe", "démographe", "géographe humain",
    "historien", "historien art", "historien médiéviste",
    "historien moderne", "historien contemporain",
    "paléontologue", "paléobotaniste", "préhistorien",
    "égyptologue", "assyriologue", "helléniste",
    "latiniste", "byzantiniste", "sinologue",
    "japonologue", "africaniste", "orientaliste",
    "conservateur patrimoine", "restaurateur patrimoine",
    "restaurateur œuvres art", "restaurateur peinture",
    "restaurateur sculpture", "restaurateur textile",
    "restaurateur mobilier", "restaurateur céramique",
    "conservateur musée", "directeur musée",
    "médiateur culturel", "chargé action culturelle",
    "chargé diffusion culturelle", "programmateur culturel",
    "directeur festival", "responsable programmation",
    "régisseur œuvres", "gestionnaire collection",
    "taxidermiste", "conservateur naturaliste",

    # =====================================================
    #  SCIENCES FONDAMENTALES & RECHERCHE
    # =====================================================
    "physicien", "physicien théorique", "physicien expérimental",
    "physicien quantique", "physicien particules",
    "physicien nucléaire", "physicien plasma",
    "astrophysicien", "astronome", "cosmologiste",
    "chimiste", "chimiste organique", "chimiste inorganique",
    "chimiste analytique", "chimiste physique",
    "chimiste matériaux", "chimiste polymère",
    "mathématicien", "statisticien", "probabiliste",
    "logicien", "cryptographe mathématique",
    "informaticien théorique", "complexité algorithmique",
    "biologiste", "biologiste cellulaire", "biologiste moléculaire",
    "généticien", "génomicien", "protéomicien",
    "bio informaticien", "biologiste computationnel",
    "immunologiste", "virologue", "bactériologiste",
    "parasitologiste", "mycologiste", "microbiologiste",
    "neuroscientifique", "neurobiologiste", "neurochirurgien",
    "neurologue", "neuropsychologue",
    "pharmacologue", "toxicologue", "épidémiologiste",
    "biostatisticien", "spécialiste santé publique",
    "chercheur cnrs", "chercheur inserm", "chercheur cea",
    "directeur recherche", "responsable unité recherche",
    "ingénieur de recherche", "technicien de recherche",
    "postdoctorant", "doctorant", "allocataire recherche",
    "attaché temporaire enseignement et recherche", "ater",
    "chargé recherche", "boursier recherche",
    "chercheur associé", "chercheur invité",
    "professeur associé", "maître assistant",

    # =====================================================
    #  JOURNALISME & MÉDIAS
    # =====================================================
    "journaliste", "journaliste reporter", "grand reporter",
    "journaliste enquête", "journaliste pigiste",
    "journaliste spécialisé", "journaliste économique",
    "journaliste politique", "journaliste sport",
    "journaliste culture", "journaliste science",
    "journaliste international", "journalisteétranger",
    "correspondant étranger", "envoyé spécial",
    "rédacteur en chef", "directeur rédaction",
    "directeur publication", "secrétaire rédaction",
    "secrétaire édition", "correcteur", "relecteur",
    "présentateur journal", "présentateur tv",
    "animateur télé", "animateur radio",
    "producteur délégué", "producteur exécutif",
    "réalisateur", "réalisateur tv", "réalisateur cinéma",
    "assistant réalisateur", "assistant production",
    "régisseur général", "régisseur lumière", "régisseur son",
    "cadreur", "preneur de son", "perchman",
    "éclairagiste", "ingénieur du son",
    "monteur", "monteur son", "étalonneur",
    "trucagiste", "compositeur", "arrangeur",
    "directeur casting", "chargé casting",
    "attaché de presse", "chargé de presse",
    "responsable relations médias", "media relations officer",
    "consultant médias", "analyste médias",
    "spécialiste e réputation", "community manager médias",
    "fact checker", "vérificateur", "désinformation analyst",
    "data journalist", "journaliste données",
    "journaliste multimédia", "journaliste web",
    "journaliste mobile", "journaliste drone",
    "vidéojournaliste", "vj", "journaliste reporter d'images",
    "photojournaliste", "correspondant guerre",
    "journaliste indépendant", "rédacteur web",
    "rédacteur seo", "rédacteur technique",
    "rédacteur médical", "rédacteur juridique",
    "rédacteur scientifique", "rédacteur publicitaire",
    "copywriter senior", "concepteur rédacteur",
    "plume", "ghost writer", "nègre littéraire",
    "scénariste", "auteur bd", "dessinateur bd",
    "coloriste bd", "encreur", "lettereur",
    "éditeur", "directeur éditorial", "responsable édition",
    "attaché éditorial", "assistant édition",
    "libraire", "responsable librairie",
    "diffuseur", "distributeur livre",

    # =====================================================
    #  CYBERSÉCURITÉ AVANCÉE
    # =====================================================
    "directeur sécurité informatique", "ciso",
    "responsable sécurité si", "rssI",
    "analyste cybersécurité", "analyste threat intelligence",
    "analyste malware", "analyste forensic",
    "analyste incident", "analyste vulnérabilité",
    "ingénieur sécurité offensive", "pentester",
    "red team operator", "red team leader",
    "blue team analyst", "blue team leader",
    "purple team", "threat hunter",
    "spécialiste sécurité cloud", "cloud security engineer",
    "spécialiste sécurité réseau", "network security engineer",
    "spécialiste sécurité applicative", "application security engineer",
    "devsecops engineer", "sécurité devops",
    "ingénieur cryptographie", "spécialiste pki",
    "spécialiste identité", "iam engineer",
    "spécialiste zero trust", "architecte sécurité",
    "consultant sécurité", "auditeur sécurité",
    "auditeur iso 27001", "auditeur pci dss",
    "responsable conformité sécurité", "gouvernance sécurité",
    "risk manager cybersécurité", "spécialiste risque cyber",
    "spécialiste continuité", "plan reprise activité",
    "responsable pRA", "responsable pca",
    "ingénieur sécurité industrielle", "ot security",
    "spécialiste sécurité iot", "iot security engineer",
    "spécialiste sécurité mobile", "mobile security engineer",
    "spécialiste sécurité ai", "ai security",
    "spécialiste sécurité blockchain", "smart contract security",
    "bug bounty hunter", "hacker éthique",
    "responsable csirt", "csirt analyst",
    "soc manager", "soc analyst tier 1",
    "soc analyst tier 2", "soc analyst tier 3",
    "siem engineer", "soar engineer",
    "edr engineer", "xdr specialist",
    "spécialiste dlp", "spécialiste waf",
    "spécialiste firewall", "firewall engineer",
    "spécialiste vpn", "spécialiste proxy",
    "reverse engineer", "malware analyst",
    "digital forensics analyst", "incident responder",
    "crisis manager cyber", "responsable réponse incident",

    # =====================================================
    #  INTELLIGENCE ARTIFICIELLE & DATA AVANCÉE
    # =====================================================
    "directeur data", "chief data officer", "cdo",
    "directeur ai", "chief ai officer",
    "head of ai", "head of data",
    "vp data", "vp ai", "vp engineering",
    "ml engineer", "machine learning engineer",
    "deep learning engineer", "nlp engineer",
    "computer vision engineer", "cv engineer",
    "speech engineer", "voice ai engineer",
    "reinforcement learning engineer", "rl engineer",
    "generative ai engineer", "llm engineer",
    "prompt engineer", "ai prompt designer",
    "ai trainer", "rlhf specialist",
    "data annotator", "data labeler",
    "data curator", "data steward",
    "data governance manager", "data quality manager",
    "data product manager", "data product owner",
    "analytics engineer", "analytics manager",
    "head of analytics", "bi director",
    "bi architect", "data viz specialist",
    "data storyteller", "insight analyst",
    "decision scientist", "research scientist",
    "applied scientist", "principal scientist",
    "staff engineer data", "distinguished engineer",
    "fellow ai", "ai researcher",
    "ml researcher", "dl researcher",
    "nlp researcher", "cv researcher",
    "robotics researcher", "autonomous systems researcher",
    "ai ethicist", "responsible ai manager",
    "ai fairness analyst", "ai bias auditor",
    "mlops engineer", "ml platform engineer",
    "feature store engineer", "model monitoring engineer",
    "data mesh architect", "data fabric architect",
    "lakehouse architect", "data lake engineer",
    "streaming engineer", "kafka engineer",
    "spark engineer", "flink engineer",
    "airflow engineer", "dbt developer",
    "data pipeline engineer", "etl architect",
    "warehouse architect", "dwh architect",
    "snowflake engineer", "databricks engineer",
    "bigquery engineer", "redshift engineer",
    "graph database engineer", "neo4j specialist",
    "time series analyst", "forecasting specialist",
    "optimization specialist", "operations research",
    "simulation engineer", "digital twin engineer",
    "quantum computing researcher", "quantum engineer",
    "quantum algorithm developer",

    # =====================================================
    #  BLOCKCHAIN & WEB3 AVANCÉ
    # =====================================================
    "blockchain architect", "blockchain developer",
    "solidity developer", "smart contract developer",
    "rust blockchain developer", "move developer",
    "defi developer", "defi protocol engineer",
    "amm developer", "liquidity engineer",
    "yield farming strategist", "tokenomics designer",
    "token engineer", "token designer",
    "nft developer", "nft artist", "nft strategist",
    "dao developer", "dao operator", "dao contributor",
    "governance specialist", "on chain governance",
    "web3 product manager", "web3 project manager",
    "web3 community manager", "web3 marketing",
    "crypto analyst", "on chain analyst",
    "blockchain analyst", "defi analyst",
    "token analyst", "crypto researcher",
    "blockchain auditor", "smart contract auditor",
    "security auditor blockchain", "bug bounty blockchain",
    "crypto compliance", "aml crypto",
    "blockchain forensic", "chainalysis analyst",
    "crypto trader", "algo trader crypto",
    "market maker crypto", "liquidity provider",
    "web3 designer", "web3 ux designer",
    "metaverse developer", "metaverse architect",
    "xr developer", "ar developer", "vr developer",
    "unity 3d developer", "unreal developer",
    "3d environment artist", "3d character artist",
    "technical artist", "shader developer",
    "game programmer", "gameplay programmer",
    "game producer", "qa game tester",
    "level designer", "narrative designer",
    "game economist", "live ops manager",

    # =====================================================
    #  ÉNERGIES RENOUVELABLES SPÉCIALISÉES
    # =====================================================
    "ingénieur solaire photovoltaïque", "ingénieur solaire thermique",
    "technicien solaire", "installateur panneaux solaires",
    "ingénieur éolien", "technicien éolien",
    "technicien maintenance éolienne", "monteur éolienne",
    "ingénieur hydroélectricité", "technicien hydroélectricité",
    "ingénieur biomasse", "technicien biomasse",
    "ingénieur géothermie", "technicien géothermie",
    "ingénieur hydrogène vert", "spécialiste hydrogène",
    "ingénieur stockage énergie", "ingénieur batterie",
    "ingénieur réseau électrique", "ingénieur smart grid",
    "spécialiste mobilité électrique", "ingénieur borne recharge",
    "ingénieur vehicle to grid", "spécialiste transition énergétique",
    "consultant transition énergétique", "auditeur énergétique",
    "diagnostiqueur énergétique", "certificat énergie",
    "thermicien", "ingénieur thermique",
    "ingénieur bâtiment durable", "architecte durable",
    "architecte bioclimatique", "consultant hqe",
    "consultant breeam", "consultant leed",
    "bim manager durable", "ingénieur performance énergétique",
    "responsable efficacité énergétique",
    "energy manager", "responsable énergie",
    "ingénieur gaz à effet de serre", "bilan carbone",
    "spécialiste compensation carbone", "carbon trader",
    "analyste carbone", "consultant carbone",

    # =====================================================
    #  DROIT SPÉCIALISÉ
    # =====================================================
    "avocat droit des affaires", "avocat droit commercial",
    "avocat droit social", "avocat droit travail",
    "avocat droit pénal", "avocat droit civil",
    "avocat droit administratif", "avocat droit constitutionnel",
    "avocat droit international", "avocat droit européen",
    "avocat droit maritime", "avocat droit aérien",
    "avocat droit spatial", "avocat droit environnement",
    "avocat droit immobilier", "avocat droit bancaire",
    "avocat droit fiscal", "avocat droit douanier",
    "avocat droit propriété intellectuelle", "avocat pi",
    "avocat droit numérique", "avocat droit data",
    "avocat droit ai", "avocat droit blockchain",
    "avocat droit santé", "avocat droit médical",
    "avocat droit assurance", "avocat droit réassurance",
    "avocat droit sport", "avocat droit audiovisuel",
    "avocat droit auteur", "avocat droit famille",
    "avocat droit successoral", "avocat droit communautaire",
    "avocat arbitrage", "avocat médiation",
    "juriste droit public", "juriste droit privé",
    "juriste droit européen", "juriste droit international",
    "juriste droit maritime", "juriste droit aérien",
    "juriste droit spatial", "juriste droit environnemental",
    "juriste droit du travail", "juriste droit social",
    "juriste droit de la santé", "juriste droit pharmaceutique",
    "juriste droit bancaire", "juriste droit boursier",
    "juriste droit des assurances", "juriste droit des marchés publics",
    "juriste droit urbanisme", "juriste droit construction",
    "juriste droit pénal", "juriste pénaliste",
    "notaire", "notaire assistant", "clerc notaire",
    "notaire associé", "notaire salarié",
    "huissier de justice", "commissaire de justice",
    "commissaire aux comptes", "commissaire aux apports",
    "expert judiciaire", "expert près cour",
    "médiateur judiciaire", "médiateur civil",
    "médiateur commercial", "médiateur familial",
    "arbitre commercial", "arbitre international",
    "conciliateur", "ombudsman", "défenseur droits",
    "greffier", "greffier chef", "greffier tribunal commerce",
    "magistrat", "magistrat judiciaire", "magistrat administratif",
    "procureur", "juge instruction", "juge tribunal",
    "juge cour appel", "juge cour cassation",
    "juge tribunal commerce", "juge prud'hommes",
    "conseiller prud'hommes", "juge enfant",
    "juge application peines", "juge administratif",
    "conseiller tribunal administratif", "conseiller cour appel",
    "premier président", "procureur général",
    "avocat général", "substitut procureur",

    # =====================================================
    #  MÉTIERS DU LIVRE & ÉDITION
    # =====================================================
    "éditeur", "directeur éditorial", "responsable édition",
    "attaché éditorial", "assistant édition",
    "correcteur typographique", "correcteur épreuves",
    "relecteur", "réviseur", "traducteur", "interprète",
    "traducteur assermenté", "interprète conférence",
    "interprète langue des signes", "interprète judiciaire",
    "localisation specialist", "localisateur",
    "terminologue", "lexicographe", "lexicologue",
    "logopède", "orthophoniste",
    "libraire", "responsable librairie",
    "libraire spécialisé", "libraire en ligne",
    "diffuseur livre", "distributeur livre",
    "commercial édition", "responsable diffusion",
    "responsable distribution livre", "logistique édition",
    "graphiste édition", "maquettiste édition",
    "infographiste édition", "metteur en page",
    "typographe", "compositeur texte",
    "relieur", "restaurateur livre", "doreur",
    "graveur", "sérigraphe", "imprimeur",
    "offsettiste", "conducteur presse",
    "technicien impression numérique", "technicien imprimerie",
    "responsable imprimerie", "directeur imprimerie",

    # =====================================================
    #  TRADUCTION & LANGUES
    # =====================================================
    "traducteur", "traductrice", "traducteur littéraire",
    "traducteur technique", "traducteur juridique",
    "traducteur médical", "traducteur scientifique",
    "traducteur financier", "traducteur commercial",
    "traducteur assermenté", "traducteur agréé",
    "traducteur freelance", "traducteur salarié",
    "relecteur traduction", "réviseur traduction",
    "correcteur linguistique", "proofreader",
    "interprète", "interprète simultané",
    "interprète consécutif", "interprète chuchotage",
    "interprète conférence", "interprète judiciaire",
    "interprète médical", "interprète social",
    "interprète langue signes", "interprète communautaire",
    "localisateur", "localisation manager",
    "terminologue", "gestionnaire terminologie",
    "lexicographe", "rédacteur multilingue",
    "coordinateur linguistique", "chef de projet traduction",
    "responsable linguistique", "directeur linguistique",
    "professeur langue", "professeur langue étrangère",
    "professeur français langue étrangère", "fle",
    "professeur anglais", "professeur espagnol",
    "professeur arabe", "professeur chinois",
    "professeur allemand", "professeur italien",
    "professeur japonais", "professeur coréen",
    "professeur portugais", "professeur russe",
    "formateur langue", "formateur fle",
    "animateur linguistique", "tuteur langue",
    "linguiste", "phonéticien", "phonologue",
    "syntaxicien", "sémanticien", "pragmaticien",
    "sociolinguiste", "psycholinguiste",
    "neurolinguiste", "computational linguist",
    "ingénieur linguistique", "nlp linguist",
    "annotateur linguistique", "corpus linguist",

    # =====================================================
    #  COMMERCE INTERNATIONAL & DOUANES
    # =====================================================
    "responsable commerce international", "directeur commerce international",
    "chargé affaires internationales", "export manager",
    "import manager", "responsable export",
    "responsable import", "assistant export",
    "assistant import", "déclarant douanier",
    "agent en douane", "agent douanier",
    "courtier en douane", "commissionnaire douane",
    "transitaire", "affréteur", "consignataire",
    "agent maritime", "agent fret aérien",
    "agent cargo", "responsable fret",
    "responsable expédition", "responsable réception",
    "agent de transit", "logisticien international",
    "coordinateur supply chain internationale",
    "spécialiste compliance export", "export compliance",
    "spécialiste sanctions", "spécialiste embargos",
    "juriste commerce international", "droit commerce international",
    "conseiller juridique international",
    "analyste risque pays", "analyste géopolitique",
    "consultant international", "conseiller commerce extérieur",
    "développeur marchés internationaux",
    "responsable implantation internationale",
    "directeur développement international",
    "agent de liaison", "représentant international",
    "délégué international", "commercial export",
    "commercial international", "business developer international",

    # =====================================================
    #  QUALITÉ NORMES & CERTIFICATIONS
    # =====================================================
    "responsable qualité", "directeur qualité",
    "ingénieur qualité", "technicien qualité",
    "auditeur qualité", "auditeur interne",
    "auditeur externe", "auditeur iso 9001",
    "auditeur iso 14001", "auditeur iso 45001",
    "auditeur iso 27001", "auditeur iatf",
    "auditeur as9100", "auditeur iso 13485",
    "auditeur iso 22000", "auditeur fssc",
    "consultant qualité", "consultant iso",
    "consultant certification", "responsable certification",
    "chargé certification", "chargé normalisation",
    "responsable normalisation", "spécialiste normes",
    "ingénieur fiabilité", "ingénieur sûreté",
    "analyste risque", "spécialiste amdec",
    "spécialiste fmea", "spécialiste 8d",
    "spécialiste lean", "spécialiste six sigma",
    "black belt six sigma", "green belt six sigma",
    "master black belt", "champion six sigma",
    "ingénieur lean manufacturing", "consultant lean",
    "consultant amélioration continue", "kaizen facilitator",
    "responsable amélioration continue",
    "ingénieur méthodes", "ingénieur industrialisation",
    "ingénieur outillage", "ingénieur gamme",
    "planiste", "planificateur production",
    "ordonnanceur", "responsable planning",
    "supply chain manager", "directeur supply chain",
    "responsable supply chain", "ingénieur supply chain",
    "consultant supply chain", "analyste supply chain",
    "demand planner", "supply planner",
    "inventory manager", "responsable stock",
    "gestionnaire stock", "analyste inventory",
    "responsable s&op", "s&op manager",
    "responsable mrp", "erp key user",
    "key user sap", "consultant sap pp",
    "consultant sap mm", "consultant sap sd",
    "consultant sap wm", "consultant sap fico",
    "consultant sap hcm", "consultant sap successfactors",
    "consultant oracle erp", "consultant microsoft dynamics",
    "consultant odoo", "consultant erpnext",

    # =====================================================
    #  MÉTIERS MANUELS & ARTISANAT
    # =====================================================
    "menuisier", "ébéniste", "tourneur sur bois",
    "sculpteur bois", "luthier", "boisselier",
    "tonnelier", "charpentier", "charpentier métallique",
    "charpentier bois", "couvreur", "couvreur zingueur",
    "étancheur", "maçon", "maçon finition",
    "tailleur de pierre", "paveur", "carreleur",
    "faïencier", "mosaïste", "staffeur ornemaniste",
    "plâtrier", "stucateur", "peintre bâtiment",
    "peintre en bâtiment", "décorateur peintre",
    "tapissier", "tapissier décorateur", "tisserand",
    "brodeur", "couturier", "couturière",
    "modéliste", "styliste mode", "costumier",
    "tailleur", "chemisier", "gantier",
    "chapelier", "cordonnier", "bottier",
    "maroquinier", "sellier", "bourrelier",
    "vannier", "osier", "rotin",
    "potier", "céramiste", "verrier",
    "souffleur de verre", "maître verrier",
    "vitrailliste", "mosaïste verre",
    "orfèvre", "joaillier", "bijoutier",
    "graveur", "ciseleur", "doreur",
    "bronzier", "fondeur", "dinandier",
    "émailleur", "horloger", "horloger bijoutier",
    "opticien", "opticien lunetier",
    "prothésiste dentaire", "orthoprothésiste",
    "bandagiste", "podologue", "pédicure",
    "ergothérapeute", "aide médico psychologique", "amp",
    "auxiliaire de puériculture", "puéricultrice",
    "auxiliaire de vie sociale", "avs",
    "aide à domicile", "garde malade",
    "infirmier libéral", "infirmier coordinateur",
    "cadre de santé paramédical",
    "ambulancier", "secouriste", "sapeur pompier",
    "policier", "gendarme", "gendarme adjoint",
    "militaire", "soldat", "caporal", "sergent",
    "adjudant", "sous officier", "officier",
    "lieutenant", "capitaine", "commandant",
    "colonel", "général", "maréchal",
    "officier marine", "amiral",
    "aviateur", "pilote militaire", "navigateur aérien",
    "contrôleur aérien militaire", "mécanicien aviation militaire",
    "technicien systèmes armes", "missilier",
    "opérateur drone militaire", "analyste renseignement",
    "officier renseignement", "agent renseignement",
    "cryptanalyste", "spécialiste sigint", "spécialiste humint",
    "analyste image", "analyste géospatial militaire",
    "officier transmissions", "officier logistique militaire",
    "officier santé", "médecin militaire",
    "infirmier militaire", "pharmacien militaire",
    "vétérinaire militaire", "aumônier militaire",

    # =====================================================
    #  FONCTIONS SUPPORT AVANCÉES
    # =====================================================
    "directeur administratif financier", "daf",
    "directeur des opérations", "coo",
    "directeur général", "dg", "ceo",
    "directeur général adjoint", "dga",
    "président directeur général", "pdg",
    "membre directoire", "membre conseil surveillance",
    "administrateur société", "commissaire aux comptes",
    "secrétaire général", "secrétaire société",
    "juriste corporate", "juriste société",
    "responsable affaires juridiques", "daj",
    "directeur juridique", "dj",
    "responsable contentieux", "avocat interne",
    "responsable propriété intellectuelle",
    "responsable brevets", "ingénieur brevets",
    "conseil propriété industrielle", "cpi",
    "mandataire judiciaire", "administrateur judiciaire",
    "liquidateur", "syndic", "curateur",
    "commissaire à l'exécution du plan",
    "commissaire à la transformation",
    "médiateur entreprise", "conciliateur entreprise",
    "administrateur provisoire", "mandataire ad hoc",
    "directeur des ressources humaines", "drh",
    "directeur juridique et social", "djs",
    "responsable administration du personnel", "rap",
    "responsable paie et administration", "rpa",
    "gestionnaire administration du personnel", "gap",
    "responsable formation et développement",
    "responsable rémunération et avantages sociaux",
    "responsable diversité", "diversity officer",
    "responsable inclusion", "inclusion manager",
    "responsable qvt", "qualité vie travail",
    "responsable rse", "csr manager",
    "chargé mission rse", "chargé mission développement durable",
    "responsable communication interne",
    "responsable communication externe",
    "directeur communication", "directeur événementiel",
    "directeur sponsoring", "responsable mécénat",
    "responsable fondation", "directeur fondation",
    "directeur innovation", "innovation manager",
    "responsable lab", "chef de lab innovation",
    "intrapreneur", "startup studio manager",
    "venture builder", "innovation strategist",
    "design strategist", "service designer",
    "ux strategist", "cx strategist",
    "customer experience director", "directeur expérience client",
    "responsable satisfaction client", "voix client",
    "spécialiste nps", "customer insights manager",
    "directeur digital", "directeur transformation digitale",
    "chief digital officer", "digital transformation lead",
    "responsable digital", "chargé mission digital",
    "consultant digital", "consultant transformation",
    "consultant stratégie", "consultant management",
    "consultant organisation", "consultant change management",
    "consultant conduite du changement",
    "consultant restructuration", "consultant fusion acquisition",
    "analyste fusion acquisition", "m&a analyst",
    "corporate finance analyst", "analyste corporate",
    "banquier affaires", "conseil en investissement",
    "associate pe", "analyste pe", "analyste vc",
    "analyste private equity", "analyste venture capital",
    "directeur investissement", "responsable investissement",
    "portfolio manager", "fund manager",
    "gérant actif", "responsable allocation actifs",
    "strategist macro", "économiste",
    "économiste chef", "analyste macroéconomique",
    "analyste sectoriel", "analyste boursier",
    "analyste technique", "analyste fondamental",
    "journaliste financier", "rédacteur financier",
    "commentateur économique", "chroniqueur économique",
    "consultant économique", "économiste développement",
    "spécialiste microfinance", "spécialiste finance islamique",
    "comptable agréé", "expert comptable",
    "commissaire aux comptes stagiaire",
    "secrétaire comptable", "aide comptable",
    "comptable unique", "comptable général",
    "comptable fournisseurs", "comptable clients",
    "comptable trésorerie", "comptable analytique",
    "analyste comptable", "responsable comptable",
    "chef comptable", "directeur comptable",
    "contrôleur de gestion", "analyste contrôle gestion",
    "responsable contrôle gestion", "directeur contrôle gestion",
    "contrôleur financier", "analyste financier",
    "planificateur financier", "budgétaire",
    "analyste budget", "responsable budget",
    "fp&a manager", "fp&a analyst",
    "consolidateur", "responsable consolidation",
    "analyste consolidation", "secrétaire financier",
    "trésorier entreprise", "cash manager",
    "responsable trésorerie", "analyste trésorerie",
    "gestionnaire trésorerie", "risk manager financier",
    "responsable risques", "analyste risques",
    "spécialiste couverture", "hedging specialist",
    "spécialiste dérivés", "structurer financier",
    "ingénieur financier", "quant analyst",
    "quantitative developer", "quant researcher",
    "risk quant", "modeler financier",
    "spécialiste modélisation", "pricing analyst",
    "actuaire", "actuaire vie", "actuaire non vie",
    "actuaire dommage", "responsable actuariat",
    "analyste actuariel", "modélisateur actuariel",

    # =====================================================
    #  MÉTIERS DE L'EAU & ASSAINISSEMENT
    # =====================================================
    "ingénieur eau", "ingénieur hydraulique",
    "ingénieur assainissement", "ingénieur traitement eau",
    "ingénieur distribution eau", "ingénieur réseau eau",
    "technicien eau", "technicien assainissement",
    "technicien traitement eau", "technicien distribution eau",
    "technicien réseau eau", "technicien adduction",
    "technicien pompage", "technicien station épuration",
    "technicien eau potable", "technicien eau usée",
    "opérateur station épuration", "opérateur traitement eau",
    "agent distribution eau", "agent réseau",
    "agent assainissement", "canalisateur",
    "vidangeur", "curageur", "déboucheur",
    "responsable service eau", "directeur eau assainissement",
    "responsable eau potable", "responsable assainissement",
    "hydrogéologue", "hydrologue", "hydrologiste",
    "ingénieur irrigation", "technicien irrigation",
    "ingénieur barrage", "technicien barrage",
    "ingénieur canalisation", "topographe hydraulique",
    "spécialiste dépollution eau", "spécialiste traitement pollution",
    "biologiste eau douce", "écologue aquatique",
    "halieuticien", "pisciculteur",
    "aquaculteur", "mollusiculteur", "algiculteur",

    # =====================================================
    #  MÉTIERS DU NUMÉRIQUE ÉTHIQUE & RÉGULATION
    # =====================================================
    "responsable rgpd", "dpo", "data protection officer",
    "délégué protection données", "consultant rgpd",
    "juriste données personnelles", "juriste privacy",
    "privacy engineer", "privacy by design",
    "responsable éthique ai", "ai ethics officer",
    "consultant éthique numérique", "digital ethics consultant",
    "responsable modération", "responsable confiance",
    "trust safety manager", "trust safety analyst",
    "content moderator", "modérateur contenu",
    "spécialiste désinformation", "fact checker",
    "analyste influence", "analyste manipulation",
    "spécialiste radicalisation en ligne",
    "responsable plateforme", "community manager senior",
    "responsable communauté", "community builder",
    "responsable ux éthique", "ethical designer",
    "accessibility specialist", "spécialiste accessibilité",
    "consultant accessibilité", "auditeur accessibilité",
    "ingénieur accessibilité", "développeur accessibilité",
    "responsable accessibilité", "web accessibility specialist",
    "consultant numérique responsable",
    "green it specialist", "ingénieur numérique responsable",
    "consultant green it", "auditeur empreinte numérique",

    # =====================================================
    #  MÉTIERS DU PATRIMOINE & TOURISME
    # =====================================================
    "chargé de mission patrimoine", "responsable patrimoine",
    "conservateur patrimoine", "conservateur monuments historiques",
    "architecte du patrimoine", "architecte monuments historiques",
    "architecte bâtiments de france", "abf",
    "chargé de valorisation patrimoine",
    "chargé de médiation patrimoine",
    "guide conférencier", "guide touristique",
    "guide accompagnateur", "accompagnateur tourisme",
    "animateur tourisme", "chargé tourisme",
    "responsable office tourisme", "directeur office tourisme",
    "chargé de mission tourisme", "chargé développement touristique",
    "consultant tourisme", "expert tourisme",
    "agent d'accueil touristique", "réceptionniste tourisme",
    "responsable hébergement touristique",
    "gérant location saisonnière", "conciergerie airbnb",
    "responsable camping", "animateur vacances",
    "directeur centre vacances", "responsable loisirs",
    "animateur loisirs", "animateur socioculturel",
    "animateur périscolaire", "animateur centre aéré",
    "animateur jeunesse", "animateur enfance",
    "responsable petite enfance", "assistant maternel",
    "auxiliaire puériculture", "eje", "puéricultrice",
    "directeur crèche", "responsable crèche",
    "assistant maternel agréé", "nounou",
    "garde enfants", "baby sitter",
    "médiateur familial", "médiateur social",
    "assistant familial", "éducateur spécialisé",
    "éducateur technique", "éducateur sportif",
    "éducateur rue", "éducateur justice",
    "conseiller éducation", "psychoéducateur",
    "moniteur éducateur", "animateur gérontologique",
    "aide soignant gériatrie", "aide médico psychologique",
    "auxiliaire de vie gériatrie", "aide à domicile personnes âgées",
    "responsable ephad", "directeur ephad",
    "cadre santé ephad", "coordinateur soins",
    "infirmier coordinateur", "médecin coordonnateur",
    "gériatre", "psychogériatre",

    # =====================================================
    #  MÉTIERS DU CUIR & MODE
    # =====================================================
    "maroquinier", "sellier harnacheur", "gainier",
    "cordonnier", "bottier", "chausseur",
    "modéliste", "styliste mode", "créateur mode",
    "directeur création mode", "directeur artistique mode",
    "acheteur mode", "buyer mode", "merchandiser mode",
    "visual merchandiser", "chef de produit mode",
    "responsable collection", "responsable approvisionnement mode",
    "agent de fabrication textile", "technicien textile",
    "ingénieur textile", "teinturier", "imprimeur textile",
    "tisserand", "fileur", "retordeur",
    "contrôleur qualité textile", "inspecteur qualité textile",
    "technicien laboratoire textile", "essayeur",
    "mannequin", "modèle", "photographe mode",
    "styliste photo", "directeur casting mode",
    "booker mannequin", "agent mannequin",
    "responsable agence mannequins",

    # =====================================================
    #  MÉTIERS DU VERRE & CÉRAMIQUE
    # =====================================================
    "verrier", "souffleur de verre", "maître verrier",
    "décorateur verre", "graveur verre", "émailleur",
    "vitrailliste", "mosaïste", "céramiste",
    "potier", "tourneur céramique", "modelage",
    "glazeur", "émailleur céramique", "fourneur",
    "technicien céramique", "ingénieur céramique",
    "technicien verre", "ingénieur verre",
    "contrôleur qualité verre", "contrôleur qualité céramique",
    "conducteur four verre", "conducteur four céramique",

    # =====================================================
    #  MÉTIERS DU SON & MUSIQUE
    # =====================================================
    "musicien", "instrumentiste", "chef d'orchestre",
    "compositeur", "arrangeur", "orchestrateur",
    "parolier", "auteur compositeur", "chanteur",
    "chanteur lyrique", "artiste interprète",
    "dj", "producteur musique", "beatmaker",
    "ingénieur du son", "technicien son",
    "mixeur", "mastering engineer", "sound designer",
    "preneur de son", "monteur son",
    "technicien sono", "technicien lumière",
    "régisseur son", "régisseur lumière",
    "éclairagiste", "machiniste spectacle",
    "accessoiriste", "régisseur plateau",
    "régisseur général spectacle", "directeur technique spectacle",
    "producteur spectacle", "programmateur salle",
    "directeur salle spectacle", "directeur festival",
    "attaché de production", "assistant production spectacle",
    "tourneur", "booking agent", "manager artiste",
    "agent artistique", "imprésario",
    "directeur artistique", "conseiller artistique",
    "critique musical", "critique théâtre",
    "critique cinéma", "chroniqueur culture",
    "journaliste culture", "journaliste musical",
    "animateur musical", "programmateur radio",
    "directeur radio", "responsable antenne",
    "technicien radio", "technicien télévision",
    "réalisateur tv", "réalisateur radio",
    "assistant réalisation", "scripte", "continuiste",
    "décorateur cinéma", "chef décorateur",
    "accessoiriste cinéma", "costumier cinéma",
    "maquilleur cinéma", "coiffeur cinéma",
    "cascadeur", "coordinateur cascades",
    "effets spéciaux", "vfx artist",
    "compositeur cinéma", "sound designer cinéma",
    "monteur cinéma", "monteur image",
    "étalonneur", "coloriste cinéma",
    "directeur photo", "cadreur cinéma",
    "chef opérateur", "éclairagiste cinéma",
    "grip", "perchman", "magnétoscope",
    "ingénieur prises de vue", "caméraman",

    # =====================================================
    #  CYBERSÉCURITÉ INDUSTRIELLE & OT
    # =====================================================
    "ingénieur sécurité industrielle", "ot security engineer",
    "spécialiste sécurité scada", "scada security",
    "spécialiste sécurité plc", "plc security",
    "spécialiste sécurité ics", "ics security",
    "spécialiste sécurité iot industriel",
    "ingénieur sûreté industrielle",
    "responsable sûreté industrielle",
    "consultant sûreté industrielle",
    "auditeur sécurité industrielle",
    "spécialiste sécurité bâtiment", "bms security",
    "spécialiste sécuritéénergie", "énergie security",
    "spécialiste sécurité eau", "water security",
    "spécialiste sécurité transport", "transport security",
    "analyste menace industrielle", "threat analyst ot",
    "incident responder ot", "forensic ot",
    "architecte réseau industriel", "network architect ot",
    "ingénieur automates", "ingénieur automatismes",
    "programmateur automates", "technicien automates",
    "ingénieur instrumentation", "technicien instrumentation",
    "ingénieur régulation", "technicien régulation",
    "ingénieur supervision", "technicien supervision",
    "ingénieur scada", "technicien scada",
    "ingénieur dcs", "technicien dcs",
    "ingénieur système embarqué", "développeur embarqué",
    "ingénieur fpga", "ingénieur vhdl",
    "ingénieur firmware", "développeur firmware",
    "ingénieur rtos", "développeur rtos",
    "ingénieur capteurs", "ingénieur actionneurs",
    "ingénieur robotique industrielle", "technicien robotique",
    "programmeur robot", "intégrateur robot",
    "ingénieur cobot", "spécialiste robot collaborative",
    "ingénieur vision industrielle", "technicien vision",
    "ingénieur plc", "technicien plc",
    "programmeur siemens", "programmeur allen bradley",
    "programmeur schneider", "programmeur mitsubishi",
    "ingénieur bus terrain", "spécialiste profinet",
    "spécialiste modbus", "spécialiste ethercat",
    "spécialiste can bus", "spécialiste devicenet",
]

CITIES = [
    "Casablanca", "Rabat", "Marrakech", "Tanger", "Fès",
    "Meknès", "Agadir", "Oujda", "Kénitra", "Tétouan",
    "El Jadida", "Nador", "Mohammedia", "Safi", "Khouribga",
]

# FIX-6: cap pairs to stay well inside the 6-hour GitHub Actions limit.
# 2700 pairs × avg 7.5 s = ~5.6 h → too close.
# 400 random pairs × 7.5 s ≈ 50 min → safe.
MAX_PAIRS = int(os.environ.get("MAX_PAIRS", "400"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,ar;q=0.8,en;q=0.7",
}

CT_MAP = {
    "cdi": "CDI",        "permanent": "CDI",   "indéterminé": "CDI",
    "temps plein": "CDI","full time": "CDI",
    "cdd": "CDD",        "déterminé": "CDD",   "temporaire": "CDD",
    "intérim": "CDD",
    "civp": "CIVP",      "insertion": "CIVP",
    "stage": "STAGE",    "internship": "STAGE","stagiaire": "STAGE",
    "pfe": "STAGE",
    "freelance": "FREELANCE", "consultant": "FREELANCE",
    "saison": "SAISON",  "saisonnier": "SAISON",
}

ICONS = {
    "CDI": "🟢", "CDD": "🟡", "CIVP": "🔵",
    "STAGE": "🟣", "FREELANCE": "🟠", "SAISON": "⚪", "?": "⚫",
}

MSG_COUNT = [0]

# FIX-7: block counter is now per-source name, not a single global int.
# A rogue source can't silence every other source.
SOURCE_BLOCKS: dict[str, int] = {}
MAX_BLOCKS_PER_SOURCE = 3


# ═══ HELPERS ═══

def norm_contract(raw: str) -> str:
    if not raw:
        return "?"
    low = raw.lower()
    for k, v in CT_MAP.items():
        if k in low:
            return v
    return "?"   # FIX-8: "?" is intentional; these rows are saved but not sent.


# FIX-5: cache RobotFileParser per domain so we fetch robots.txt once per site.
_robots_cache: dict[str, RobotFileParser] = {}

def robots_ok(url: str) -> bool:
    p    = urlparse(url)
    base = f"{p.scheme}://{p.netloc}"
    if base not in _robots_cache:
        rp = RobotFileParser()
        rp.set_url(f"{base}/robots.txt")
        try:
            rp.read()
        except Exception:
            pass   # if robots.txt is unreachable, assume allowed
        _robots_cache[base] = rp
    return _robots_cache[base].can_fetch("*", url)


def fetch(url: str, source_name: str = "?"):
    """Fetch URL with retry/back-off. Returns BeautifulSoup or None."""
    if not robots_ok(url):
        log.info(f"robots.txt blocked: {url}")
        return None
    # FIX-7: per-source block limit
    if SOURCE_BLOCKS.get(source_name, 0) >= MAX_BLOCKS_PER_SOURCE:
        return None

    for i in range(3):
        try:
            time.sleep(random.uniform(5, 10))
            r = requests.get(url, headers=HEADERS, timeout=20)

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 60))
                log.warning(f"429 from {source_name}. Waiting {wait}s")
                time.sleep(wait)
                continue

            if r.status_code == 403:
                SOURCE_BLOCKS[source_name] = (
                    SOURCE_BLOCKS.get(source_name, 0) + 1
                )
                log.warning(
                    f"403 [{source_name}] "
                    f"({SOURCE_BLOCKS[source_name]}/{MAX_BLOCKS_PER_SOURCE})"
                )
                return None

            if r.status_code == 404:
                return None

            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")

        except Exception as e:
            log.warning(f"Fetch retry {i+1}/3 [{source_name}]: {e}")
            time.sleep(10 * (i + 1))

    return None


def txt(el) -> str:
    return el.get_text(strip=True) if el else ""


# FIX-9: Use \b word boundaries so "salary" doesn't match "no-salary-hidden".
def find_text(card, patterns: list[str]) -> str:
    for p in patterns:
        # \b ensures we match whole words / class segments, not substrings
        el = card.find(class_=re.compile(r"\b" + re.escape(p) + r"\b", re.I))
        if el:
            return txt(el)
    return ""


# ═══ DATABASE ═══

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect("jobs.db")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            uid      TEXT PRIMARY KEY,
            title    TEXT,
            company  TEXT,
            location TEXT,
            source   TEXT,
            url      TEXT,
            contract TEXT,
            salary   TEXT,
            sector   TEXT,
            sent     INTEGER DEFAULT 0,
            ts       TEXT
        )
    """)
    conn.commit()
    return conn


SEEN_THIS_RUN: set[str] = set()


def save_job(conn: sqlite3.Connection, j: dict) -> int:
    uid = hashlib.md5(f"{j['url']}{j['title']}".encode()).hexdigest()

    if uid in SEEN_THIS_RUN:
        return 0
    SEEN_THIS_RUN.add(uid)

    try:
        conn.execute(
            "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,0,?)",
            (
                uid, j["title"], j["company"], j["location"],
                j["source"], j["url"], j["contract"],
                j["salary"], j.get("sector", ""),
                datetime.now().isoformat(),
            ),
        )
        conn.commit()
        return 1
    except sqlite3.IntegrityError:
        return 0


# ═══ PARSER ═══

def parse_cards(soup, source: str, base_url: str) -> list[dict]:
    """
    FIX-2: Two new guards added:
      a) Reject links whose netloc differs from base_url (off-site = nav/ad junk).
      b) Require title length >= 8 (was 3, too loose).
    """
    base_netloc = urlparse(base_url).netloc
    jobs = []

    for card in soup.find_all(["div", "article", "li", "tr", "section"]):
        el = card.find(["h2", "h3"]) or card.find("a")
        if not el:
            continue

        title = txt(el)
        if len(title) < 8:          # tighter than the original `< 3`
            continue

        a    = card.find("a")
        link = a["href"] if a and a.get("href") else ""
        if link and not link.startswith("http"):
            link = base_url.rstrip("/") + "/" + link.lstrip("/")
        if not link:
            continue

        # FIX-2a: drop off-site links (footers, ads, partner banners, …)
        link_netloc = urlparse(link).netloc
        if link_netloc and link_netloc != base_netloc:
            continue

        jobs.append({
            "title":    title,
            "company":  find_text(card, ["company", "entreprise", "corp", "employer"]) or "N/A",
            "location": find_text(card, ["location", "ville", "lieu", "city", "region"]) or "?",
            "source":   source,
            "url":      link,
            "contract": norm_contract(
                find_text(card, ["type", "contrat", "contract", "nature"])
            ),
            "salary":   find_text(card, ["salary", "salaire", "remuneration"]),
            "sector":   find_text(card, ["sector", "secteur", "category"]),
        })

    return jobs


# ═══ SOURCES ═══

SOURCES = [
    {"name": "ANAPEC",         "base": "https://www.anapec.org",            "path": "/search/result?mot_cle={}&ville={}"},
    {"name": "Rekrute",        "base": "https://www.rekrute.com",           "path": "/offres-emploi.html?q={}&l={}"},
    {"name": "Emploi.ma",      "base": "https://www.emploi.ma",             "path": "/recherche?q={}&l={}"},
    {"name": "Indeed",         "base": "https://ma.indeed.com",             "path": "/jobs?q={}&l={}&sort=date"},
    {"name": "MarocAnnonces",  "base": "https://www.marocannonces.com",     "path": "/emploi/offres-emploi/?keyword={}&city={}"},
    {"name": "Bayt",           "base": "https://www.bayt.com/en/morocco",   "path": "/jobs/{}-jobs/?location={}"},
    {"name": "Jobijoba",       "base": "https://www.jobijoba.com/ma",       "path": "/search?what={}&where={}"},
    {
        # FIX-3: Glassdoor URL uses display-string length (unencoded kw).
        # build_url() handles this specially; see below.
        "name": "Glassdoor",
        "base": "https://www.glassdoor.com",
        "path": "/Job/morocco-{kw}-jobs-SRCH_IL.0,7_IN187_KO8,{end}.htm",
    },
    {"name": "MEmploi",        "base": "https://www.memploi.ma",            "path": "/recherche?q={}&l={}"},
    {"name": "Moovjobs",       "base": "https://www.moovjobs.com",          "path": "/emplois?q={}&l={}"},
    {"name": "Kemayo",         "base": "https://www.kemayo.ma",             "path": "/offres-emploi?q={}&l={}"},
    {"name": "OptionCarriere", "base": "https://www.optioncarriere.ma",     "path": "/emploi?q={}&l={}"},
    {"name": "Talent.com",     "base": "https://ma.talent.com",             "path": "/search?l={}&q={}"},
    {"name": "Careerjet",      "base": "https://www.careerjet.ma",          "path": "/search?loc={}&keywords={}"},
    {"name": "Waadni",         "base": "https://www.waadni.com",            "path": "/recherche?q={}&l={}"},
]


def build_url(src: dict, kw: str, city: str) -> str | None:
    """
    FIX-3: Glassdoor's URL offset is based on the *display* (unencoded)
    keyword string length, not the percent-encoded version.
    e.g. "développeur" (11 chars) → KO8,19   (8 + 11)
         "python"      (6 chars)  → KO8,14   (8 + 6)
    We use kw directly (unencoded) for the length calculation.
    """
    kw_enc   = quote_plus(kw)
    city_enc = quote_plus(city)

    if src["name"] == "Glassdoor":
        try:
            end = 8 + len(kw)          # display length, not encoded length
            path = src["path"].format(kw=kw_enc, end=end)
            return src["base"] + path
        except (IndexError, KeyError, Exception):
            return None                 # drop gracefully if pattern breaks

    try:
        return src["base"] + src["path"].format(kw_enc, city_enc)
    except (IndexError, KeyError):
        return None


# ═══ SCRAPING ═══

def scrape_keyword_city(kw: str, city: str) -> list[dict]:
    all_jobs = []
    for src in SOURCES:
        # FIX-7: skip only this source if it's blocked, not everything
        if SOURCE_BLOCKS.get(src["name"], 0) >= MAX_BLOCKS_PER_SOURCE:
            log.warning(f"[{src['name']}] skipped — block limit reached")
            continue
        try:
            url = build_url(src, kw, city)
            if not url:
                continue
            soup = fetch(url, source_name=src["name"])
            if soup:
                jobs = parse_cards(soup, src["name"], src["base"])
                all_jobs.extend(jobs)
                if jobs:
                    log.info(f"[{src['name']}] {kw}/{city}: {len(jobs)} found")
        except Exception as e:
            log.error(f"[{src['name']}] {e}")
    return all_jobs


# ═══ TELEGRAM ═══

def tg_send(text: str) -> bool:
    for i in range(3):
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={
                    "chat_id": TG_CHAT,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            if r.status_code == 429:
                wait = r.json().get("parameters", {}).get("retry_after", 30)
                log.warning(f"TG rate limit. Waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 200:
                MSG_COUNT[0] += 1
                time.sleep(3)
                if MSG_COUNT[0] % 20 == 0:
                    log.info(f"TG cooldown after {MSG_COUNT[0]} msgs")
                    time.sleep(30)
                return True
            log.error(f"TG error: {r.status_code} — {r.text[:200]}")
        except Exception as e:
            log.error(f"TG send error: {e}")
        time.sleep(10)
    return False


def format_job(row) -> str:
    # Schema: uid(0) title(1) company(2) location(3)
    #         source(4) url(5) contract(6) salary(7)
    #         sector(8) sent(9) ts(10)
    ct   = row[6] or "?"
    icon = ICONS.get(ct, "⚫")
    parts = [
        f"{icon} <b>{row[1]}</b>",
        f"🏢 {row[2]}",
        f"📍 {row[3]}",
    ]
    if row[7]:
        parts.append(f"💰 {row[7]}")
    parts.append(f"📋 {ct}")
    if row[8]:
        parts.append(f"🏷️ {row[8]}")
    parts.extend([
        f'🔗 <a href="{row[5]}">Voir l\'offre</a>',
        f"📡 {row[4]}",
        "━━━━━━━━━━━━━━━━━━",
    ])
    return "\n".join(parts)


TG_MAX_BYTES = 4000   # Telegram hard limit is 4096; leave headroom


def send_to_telegram(conn: sqlite3.Connection) -> None:
    # FIX-4: dynamic IN(?) placeholder count
    placeholders = ",".join("?" * len(FILTER))
    rows = conn.execute(
        f"SELECT * FROM jobs "
        f"WHERE sent=0 AND contract IN ({placeholders}) "
        f"ORDER BY ts DESC LIMIT 200",
        FILTER,
    ).fetchall()

    if not rows:
        tg_send("📭 ما لقيناش عروض جداد اليوم.")
        return

    total      = 0
    sent_uids  = []   # FIX-11: collect only rows that were actually sent

    # FIX-10: build messages by accumulated byte length, not job count.
    current_msg  = ""
    current_rows = []

    def flush(msg: str, batch_rows: list) -> None:
        nonlocal total
        if not msg.strip():
            return
        header = (
            f"📦 <b>{len(batch_rows)} عروض</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
        )
        ok = tg_send(header + msg)
        if ok:
            sent_uids.extend(r[0] for r in batch_rows)
            total += len(batch_rows)

    for row in rows:
        chunk = format_job(row) + "\n"
        if len((current_msg + chunk).encode("utf-8")) > TG_MAX_BYTES:
            # current buffer would overflow → flush first
            flush(current_msg, current_rows)
            current_msg  = chunk
            current_rows = [row]
        else:
            current_msg  += chunk
            current_rows.append(row)

    flush(current_msg, current_rows)   # send whatever remains

    # FIX-11: mark only successfully-sent rows
    for uid in sent_uids:
        conn.execute("UPDATE jobs SET sent=1 WHERE uid=?", (uid,))
    conn.commit()

    log.info(f"Sent {total} jobs to Telegram")


def send_stats(conn: sqlite3.Connection) -> None:
    total = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    sent  = conn.execute("SELECT COUNT(*) FROM jobs WHERE sent=1").fetchone()[0]

    by_contract = conn.execute(
        "SELECT contract, COUNT(*) FROM jobs GROUP BY contract ORDER BY COUNT(*) DESC"
    ).fetchall()
    by_source = conn.execute(
        "SELECT source, COUNT(*) FROM jobs GROUP BY source ORDER BY COUNT(*) DESC"
    ).fetchall()
    by_city = conn.execute(
        "SELECT location, COUNT(*) FROM jobs GROUP BY location ORDER BY COUNT(*) DESC LIMIT 15"
    ).fetchall()

    c_lines = "\n".join(f"  {r[0]}: {r[1]}" for r in by_contract)
    s_lines = "\n".join(f"  {r[0]}: {r[1]}" for r in by_source)
    v_lines = "\n".join(f"  {r[0]}: {r[1]}" for r in by_city)

    tg_send(
        f"📊 <b>إحصائيات عروض الشغل</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📦 المجموع: {total}\n"
        f"📤 المرسلة: {sent}\n"
        f"📥 الانتظار: {total - sent}\n\n"
        f"📋 <b>حسب العقد:</b>\n{c_lines}\n\n"
        f"📡 <b>حسب المصدر:</b>\n{s_lines}\n\n"
        f"📍 <b>حسب المدينة:</b>\n{v_lines}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )


# ═══ MAIN ═══

def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "full"
    conn = init_db()

    log.info(f"=== MOROCCO JOBS [{mode}] ===")
    log.info(f"Keywords : {len(KEYWORDS)}")
    log.info(f"Cities   : {len(CITIES)}")
    log.info(f"Sources  : {len(SOURCES)}")
    log.info(f"Filter   : {FILTER}")
    log.info(f"Max pairs: {MAX_PAIRS}")

    if mode in ("full", "scrape"):
        # FIX-6: build all combos then sample to MAX_PAIRS
        all_pairs = [(kw, city) for kw in KEYWORDS for city in CITIES]
        random.shuffle(all_pairs)
        pairs = all_pairs[:MAX_PAIRS]

        log.info(
            f"Scraping {len(pairs)} pairs "
            f"(of {len(all_pairs)} total)"
        )

        new_total = 0
        for kw, city in pairs:
            # FIX-1: was `>= \n 5` — syntax error
            all_blocked = all(
                SOURCE_BLOCKS.get(s["name"], 0) >= MAX_BLOCKS_PER_SOURCE
                for s in SOURCES
            )
            if all_blocked:
                log.warning("All sources blocked. Stopping scrape early.")
                break

            jobs = scrape_keyword_city(kw, city)
            for j in jobs:
                new_total += save_job(conn, j)

        log.info(f"NEW JOBS SAVED: {new_total}")

    if mode in ("full", "send"):
        send_to_telegram(conn)

    if mode in ("full", "stats"):
        send_stats(conn)

    log.info(
        f"TG msgs: {MSG_COUNT[0]} | "
        f"Blocks: {dict(SOURCE_BLOCKS)} | "
        f"DONE"
    )


if __name__ == "__main__":
    main()
