import { Test, TestingModule } from '@nestjs/testing';
import { ReportDetailsController } from './report-details.controller';

describe('ReportDetailsController', () => {
  let controller: ReportDetailsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [ReportDetailsController],
    }).compile();

    controller = module.get<ReportDetailsController>(ReportDetailsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
