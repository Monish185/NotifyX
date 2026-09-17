-- AlterTable
ALTER TABLE "in_app_notifications" ADD COLUMN "deliveryId" TEXT;

-- CreateIndex
CREATE UNIQUE INDEX "in_app_notifications_deliveryId_key" ON "in_app_notifications"("deliveryId");
